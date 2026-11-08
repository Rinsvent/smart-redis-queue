package redisqueue

import (
	"github.com/redis/go-redis/v9"
)

// TagSeparator — разделитель тегов/групп в ARGV add-скрипта и в Redis-индексах.
// Значение не должно содержать этот символ. Выбран `#`: реже встречается в id/slug, чем `,` или `/`.
const TagSeparator = "#"

// DefaultGroup — группа по умолчанию, если Task.Groups пуст (legacy / без rate-limit группы).
const DefaultGroup = "_default"

// DefaultRemoveByTagLimit — лимит RemoveByTag по умолчанию.
const DefaultRemoveByTagLimit = 100

// MaxRemoveByTagLimit — верхняя граница лимита RemoveByTag (защита от долгой блокировки Lua).
const MaxRemoveByTagLimit = 1000

// Статусы удаления задачи (Remove / RemoveByTag).
const (
	RemovalStatusMissing    = "missing"
	RemovalStatusInProgress = "inprogress"
	RemovalStatusRemoved    = "removed"
)

// getAddScript — добавление задач (батч). Один скрипт: single-queue и exchange multi-dest.
//
// Single (Producer.Publish):
//
//	ARGV[1] = queue name
//	ARGV[2] = taskCount
//	ARGV[3] = bpCount
//	ARGV[4 .. 3+2*bpCount] = пары tag, limit
//	далее по 7 на задачу: id, partition, priority, scheduled, payload, tags, groups
//
// Exchange (Exchange.Publish atomic):
//
//	ARGV[1] = "ex"
//	ARGV[2] = exchange name
//	ARGV[3] = version (пусто = без GET ver; иначе check → ERR_EXCHANGE_VERSION)
//	ARGV[4] = destCount
//	ARGV[5 .. 4+destCount] = dest queue names
//	далее как single с ARGV[1]=queue: taskCount, bpCount, bp pairs, tasks
//
// Возвращает: индексы NX-дубликатов; или {'!', 'backpressure', ...}; exchange при успехе {}.
var addScript = redis.NewScript(`
-- bpBase: индекс первого tag; taskBase: индекс первого task id (1-based ARGV).
local function check_backpressure(queueName, taskCount, bpCount, bpBase)
	if bpCount <= 0 then
		return nil
	end
	for bi = 0, bpCount - 1 do
		local tag = ARGV[bpBase + bi * 2]
		local limit = tonumber(ARGV[bpBase + 1 + bi * 2]) or 0
		if tag ~= "" and limit > 0 then
			local n = redis.call('SCARD', "queue:" .. queueName .. ":tag:" .. tag)
			if n + taskCount > limit then
				return {'!', 'backpressure', tag, tostring(n), tostring(limit)}
			end
		end
	end
	return nil
end

-- Задача: 8 ARGV — id, partition, partitionPriority, queuePriority, scheduled, payload, tags, groups.
-- QueuePriority==0 → legacy-ключи; >0 → queue:{q}:qp{N}:…
local function add_tasks_to_queue(queueName, taskCount, taskBase)
	local groupsKey = "queue:" .. queueName .. ":groups"
	local queuePrioritiesKey = "queue:" .. queueName .. ":priorities"

	local function qpPrefix(qp)
		qp = tonumber(qp) or 0
		if qp == 0 then return "" end
		return "qp" .. tostring(qp) .. ":"
	end

	local function ensureGroup(group)
		if not redis.call('ZSCORE', groupsKey, group) then
			redis.call('ZADD', groupsKey, 0, group)
		end
	end

	local function ensurePartitionReady(partitionCode, pref)
		local partGroupsKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":groups"
		local pgroups = redis.call('SMEMBERS', partGroupsKey)
		if #pgroups == 0 then
			pgroups = {"_default"}
			redis.call('SADD', partGroupsKey, "_default")
		end
		for gi = 1, #pgroups do
			local g = pgroups[gi]
			ensureGroup(g)
			redis.call('ZADD', "queue:" .. queueName .. ":" .. pref .. "group:" .. g .. ":ready", 0, partitionCode)
		end
	end

	local debPrefix = "deb!"
	local notAddedItems = {}
	local addedCount = 0

	for i = 0, taskCount - 1 do
		local base = taskBase + i * 8
		local taskId = ARGV[base]
		local partitionCode = ARGV[base + 1]
		if partitionCode == "" then
			partitionCode = "base"
		end
		local priority = ARGV[base + 2]
		local queuePriority = tonumber(ARGV[base + 3]) or 0
		local scheduled = tonumber(ARGV[base + 4])
		local payload = ARGV[base + 5]
		local tagsStr = ARGV[base + 6] or ""
		local groupsStr = ARGV[base + 7] or ""
		local pref = qpPrefix(queuePriority)

		local payloadKey = "queue:" .. queueName .. ":payload:" .. taskId
		local partitionKey = "queue:" .. queueName .. ":partition:" .. taskId
		local priorityKey = "queue:" .. queueName .. ":priority:" .. taskId
		local qpriorityKey = "queue:" .. queueName .. ":qpriority:" .. taskId
		local partitionsKey = "queue:" .. queueName .. ":" .. pref .. "partitions"
		local isDebounce = taskId:sub(1, #debPrefix) == debPrefix

		local ok = redis.call('SET', payloadKey, payload, 'NX')
		if ok then
			if queuePriority > 0 then
				redis.call('MSET', partitionKey, partitionCode, priorityKey, priority, qpriorityKey, tostring(queuePriority))
				redis.call('ZADD', queuePrioritiesKey, queuePriority, tostring(queuePriority))
			else
				redis.call('MSET', partitionKey, partitionCode, priorityKey, priority)
			end

			local partitionQueueKey = "queue:" .. queueName .. ":" .. pref .. "partition:" .. partitionCode .. ":" .. priority
			redis.call('ZADD', partitionQueueKey, scheduled, taskId)
			redis.call('SADD', partitionsKey, partitionCode)

			local prioritiesKey = "queue:" .. queueName .. ":" .. pref .. "partition:" .. partitionCode .. ":priorities"
			redis.call('ZADD', prioritiesKey, priority, priority)

			if tagsStr ~= "" then
				local tagsKey = "queue:" .. queueName .. ":tags:" .. taskId
				local raceTagsKey = "queue:" .. queueName .. ":race:tags:" .. taskId
				local racePrefix = "race!"
				for tag in string.gmatch(tagsStr, "[^#]+") do
					if tag ~= "" then
						redis.call('SADD', "queue:" .. queueName .. ":tag:" .. tag, taskId)
						redis.call('SADD', tagsKey, tag)
						if tag:sub(1, #racePrefix) == racePrefix then
							redis.call('SADD', raceTagsKey, tag)
						end
					end
				end
			end

			local partGroupsKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":groups"
			local hasGroup = false
			if groupsStr ~= "" then
				for group in string.gmatch(groupsStr, "[^#]+") do
					if group ~= "" then
						hasGroup = true
						ensureGroup(group)
						redis.call('ZADD', "queue:" .. queueName .. ":" .. pref .. "group:" .. group .. ":ready", 0, partitionCode)
						redis.call('SADD', partGroupsKey, group)
					end
				end
			end
			if not hasGroup then
				ensureGroup("_default")
				redis.call('ZADD', "queue:" .. queueName .. ":" .. pref .. "group:_default:ready", 0, partitionCode)
				redis.call('SADD', partGroupsKey, "_default")
			end
			addedCount = addedCount + 1
		elseif isDebounce then
			local debounceKey = "queue:" .. queueName .. ":debounce:" .. taskId
			local existingMeta = redis.call('MGET', partitionKey, priorityKey, qpriorityKey)
			local existingPart = existingMeta[1]
			if existingPart == false then
				existingPart = "base"
			end
			local existingPrio = existingMeta[2]
			if existingPrio == false then
				existingPrio = "0"
			end
			local existingQp = 0
			if existingMeta[3] ~= false then
				existingQp = tonumber(existingMeta[3]) or 0
			end
			local oldPref = qpPrefix(existingQp)
			local oldQueueKey = "queue:" .. queueName .. ":" .. oldPref .. "partition:" .. existingPart .. ":" .. existingPrio
			local inQueue = redis.call('ZSCORE', oldQueueKey, taskId)

			redis.call('SET', payloadKey, payload)

			if inQueue ~= false then
				local moved = existingPart ~= partitionCode or tostring(existingPrio) ~= tostring(priority) or existingQp ~= queuePriority
				if moved then
					redis.call('ZREM', oldQueueKey, taskId)
				end
				if queuePriority > 0 then
					redis.call('MSET', partitionKey, partitionCode, priorityKey, priority, qpriorityKey, tostring(queuePriority))
					redis.call('ZADD', queuePrioritiesKey, queuePriority, tostring(queuePriority))
				else
					redis.call('MSET', partitionKey, partitionCode, priorityKey, priority)
					redis.call('DEL', qpriorityKey)
				end
				local partitionQueueKey = "queue:" .. queueName .. ":" .. pref .. "partition:" .. partitionCode .. ":" .. priority
				redis.call('ZADD', partitionQueueKey, scheduled, taskId)
				redis.call('SADD', partitionsKey, partitionCode)
				local prioritiesKey = "queue:" .. queueName .. ":" .. pref .. "partition:" .. partitionCode .. ":priorities"
				redis.call('ZADD', prioritiesKey, priority, priority)
				ensurePartitionReady(partitionCode, pref)
				redis.call('DEL', debounceKey)
			else
				if queuePriority > 0 then
					redis.call('MSET', partitionKey, partitionCode, priorityKey, priority, qpriorityKey, tostring(queuePriority), debounceKey, tostring(scheduled))
				else
					redis.call('DEL', qpriorityKey)
					redis.call('MSET', partitionKey, partitionCode, priorityKey, priority, debounceKey, tostring(scheduled))
				end
			end
		else
			notAddedItems[#notAddedItems + 1] = i
		end
	end

	if addedCount > 0 then
		local _qd = tonumber(redis.call('ZINCRBY', 'srq:queues', addedCount, queueName))
		if _qd < 0 then
			redis.call('ZADD', 'srq:queues', 0, queueName)
		end
	end

	return notAddedItems
end

-- маркеры режима вне namespace имён очередей
if ARGV[1] == "__srq_ex" then
	local exchangeName = ARGV[2]
	local wantVer = ARGV[3] or ""
	if wantVer ~= "" then
		local cur = redis.call("GET", "srq:exchange:" .. exchangeName .. ":ver")
		if cur == false or cur == nil then
			cur = "0"
		end
		if tostring(cur) ~= wantVer then
			return redis.error_reply("ERR_EXCHANGE_VERSION")
		end
	end

	local destCount = tonumber(ARGV[4]) or 0
	local destBase = 5
	local afterDests = destBase + destCount
	local taskCount = tonumber(ARGV[afterDests]) or 0
	local bpCount = tonumber(ARGV[afterDests + 1]) or 0
	local bpBase = afterDests + 2
	local taskBase = bpBase + bpCount * 2

	if destCount < 1 or taskCount < 1 then
		return {}
	end

	for di = 0, destCount - 1 do
		local bp = check_backpressure(ARGV[destBase + di], taskCount, bpCount, bpBase)
		if bp then
			return bp
		end
	end
	for di = 0, destCount - 1 do
		add_tasks_to_queue(ARGV[destBase + di], taskCount, taskBase)
	end
	return {}
end

-- journal: ARGV = "__srq_jn", cursorKey, newCursor, queueName, taskCount, bpCount, ...
if ARGV[1] == "__srq_jn" then
	local cursorKey = ARGV[2]
	local newCursor = ARGV[3] or ""
	if cursorKey ~= "" and newCursor ~= "" then
		local cur = redis.call("GET", cursorKey)
		if cur ~= false and cur ~= nil and tostring(cur) >= newCursor then
			return redis.error_reply("ERR_JOURNAL_CURSOR_REWIND")
		end
	end
	local queueName = ARGV[4]
	local taskCount = tonumber(ARGV[5]) or 0
	local bpCount = tonumber(ARGV[6]) or 0
	local bpBase = 7
	local taskBase = bpBase + bpCount * 2
	local bp = check_backpressure(queueName, taskCount, bpCount, bpBase)
	if bp then
		return bp
	end
	local res = add_tasks_to_queue(queueName, taskCount, taskBase)
	if cursorKey ~= "" and newCursor ~= "" then
		redis.call("SET", cursorKey, newCursor)
	end
	return res
end

local queueName = ARGV[1]
local taskCount = tonumber(ARGV[2])
local bpCount = tonumber(ARGV[3]) or 0
local bpBase = 4
local taskBase = 4 + bpCount * 2
local bp = check_backpressure(queueName, taskCount, bpCount, bpBase)
if bp then
	return bp
end
return add_tasks_to_queue(queueName, taskCount, taskBase)
`)

// geLockScript возвращает Lua скрипт для блокировки ключей (батч)
// ARGV[1] = queue name
// ARGV[2] = количество задач
// Далее requestId:
//
// ARGV[i] = requestId
var lockScript = redis.NewScript(`
local queueName = ARGV[1]
local taskCount = tonumber(ARGV[2])

local notAddedItems = {}

for i = 0, taskCount - 1 do
    local base = 3 + i
    local taskId = ARGV[base]

    local payloadKey = "queue:" .. queueName .. ":payload:" .. taskId

    -- SET NX возвращает false при занятом ключе (не 0).
    local ok = redis.call('SET', payloadKey, 0, 'NX')
    if not ok then
		notAddedItems[#notAddedItems + 1] = i
    end
end

return notAddedItems
`)

// getGetScript возвращает Lua скрипт для получения до prefetchCount задач
// ARGV[1] = queue name
// ARGV[2] = consumer ID
// ARGV[3] = prefetch count
// ARGV[4] = checkDeadConsumerLocksOnGet ("1" / "0")
// ARGV[5] = algorithm ("" = RL выкл); ARGV[6]=gRateN + пары; затем pRateN + пары
// После rates: ARGV[argIdx] = concurrencyTags ("1" / "0")
// ARGV[argIdx+1] = samePartition ("1" / "0") — добор prefetch из партиции первой задачи
// Возвращает: {taskId, partition, payload, rejectCount, scoreMs, sharedPairs, ...} или {}
var getScript = redis.NewScript(`
local queueName = ARGV[1]
local consumerId = ARGV[2]
local prefetchCount = tonumber(ARGV[3]) or 1
local checkDeadConsumerLocks = ARGV[4] == "1"
if prefetchCount < 1 then
    prefetchCount = 1
end

-- F4: algorithm + слои. Пустой algorithm / нулевые rates → без RL call.
local rlAlgo = ARGV[5] or ""
local argIdx = 6
local function parseRates()
	local n = tonumber(ARGV[argIdx]) or 0
	argIdx = argIdx + 1
	local rates = {}
	for i = 1, n do
		local durMs = tonumber(ARGV[argIdx]) or 0
		local value = tonumber(ARGV[argIdx + 1]) or 0
		argIdx = argIdx + 2
		if durMs > 0 and value > 0 then
			rates[#rates + 1] = {
				durMs = durMs,
				value = value,
				refill = value / (durMs / 1000.0),
				durSec = durMs / 1000.0
			}
		end
	end
	return rates
end
local gRates = parseRates()
local pRates = parseRates()
if rlAlgo == "" then
	gRates = {}
	pRates = {}
end

-- F5: concurrency tags (race!) — только при opt-in (хелперы после deferredPartitions)
local concurrencyTags = (ARGV[argIdx] or "") == "1"
local samePartition = (ARGV[argIdx + 1] or "") == "1"
local raceLocksKey = "queue:" .. queueName .. ":race:locks"
local raceLocks = nil
local raceSkips = 0
local maxRaceSkips = 64
local raceBudgetExhausted = false
local raceSharedPrefix = "race!s!"
local raceBlockPrefix = "race!b!"
local blockReadyValue = "ready"
local busyRaceTags = {}

-- Вычисляем текущее время
local currentTime = redis.call('TIME')
local currentUnix = currentTime[1]
local now = currentUnix * 1000 + math.floor(currentTime[2] / 1000)
local nowSec = now / 1000.0

-- Формируем ключи
local consumersKey = "queue:" .. queueName .. ":consumers"
local consumerKey = "queue:" .. queueName .. ":consumer:" .. consumerId
local consumerTasksKey = "queue:" .. queueName .. ":consumer:" .. consumerId .. ":tasks"
local queuePrioritiesKey = "queue:" .. queueName .. ":priorities"
-- как partition priorities: ZREVRANGE high→low; один снимок на Get
local queuePriorities = redis.call('ZREVRANGE', queuePrioritiesKey, 0, -1)
-- qp >= этого уже исчерпаны в этом Get; nil = ещё ничего не скипаем
local skipQueuePriorityAtOrAbove = nil

-- currentPref: "" (legacy qp=0) или "qpN:"
local currentPref = ""
local function qpPrefix(qp)
	qp = tonumber(qp) or 0
	if qp == 0 then return "" end
	return "qp" .. tostring(qp) .. ":"
end

local acquiredPartitionLocks = {}
local partitionHadTask = {}
-- R4/D4: ключ = pref..partition — future в qp10 не блокирует ready в qp5/legacy
local deferredPartitions = {}
local function deferPartition(partition)
	deferredPartitions[currentPref .. partition] = true
end
local function isPartitionDeferred(partition)
	return deferredPartitions[currentPref .. partition] == true
end

local function ensureRaceLocks()
	if raceLocks ~= nil then
		return
	end
	raceLocks = {}
	local flat = redis.call('HGETALL', raceLocksKey)
	for i = 1, #flat, 2 do
		raceLocks[flat[i]] = flat[i + 1]
	end
end

-- true, sharedPairs|nil; false, nil при race busy (partition deferred).
-- race!s!{id}: если queue:{q}:shared:{id} есть — lock не берём, blob в ответ.
-- EVAL атомарный: между проверкой map и HSET другой клиент не вклинится.
-- HSET (не HSETNX): снимок raceLocks уже полный для этого скрипта.
local function tryAcquireRace(taskId, partition)
	if not concurrencyTags then
		return true, nil
	end
	local raceTagsKey = "queue:" .. queueName .. ":race:tags:" .. taskId
	local tags = redis.call('SMEMBERS', raceTagsKey)
	if #tags == 0 then
		return true, nil
	end

	local needLock = {}
	local sharedIds = {}
	local sharedKeys = {}
	local blockIds = {}
	local blockKeys = {}
	local blockTags = {}
	for i = 1, #tags do
		local t = tags[i]
		if busyRaceTags[t] then
			deferPartition(partition)
			raceSkips = raceSkips + 1
			if raceSkips >= maxRaceSkips then
				raceBudgetExhausted = true
			end
			return false, nil
		end
		if t:sub(1, #raceSharedPrefix) == raceSharedPrefix then
			local id = t:sub(#raceSharedPrefix + 1)
			if id ~= "" then
				sharedIds[#sharedIds + 1] = id
				sharedKeys[#sharedKeys + 1] = "queue:" .. queueName .. ":shared:" .. id
			end
		elseif t:sub(1, #raceBlockPrefix) == raceBlockPrefix then
			local id = t:sub(#raceBlockPrefix + 1)
			if id ~= "" then
				blockIds[#blockIds + 1] = id
				blockKeys[#blockKeys + 1] = "queue:" .. queueName .. ":block:" .. id
				blockTags[#blockTags + 1] = t
			end
		else
			needLock[#needLock + 1] = t
		end
	end

	-- race!b!: wait (не ready) → skip; ready → без лока; нет ключа → mutex
	if #blockKeys > 0 then
		local bgot = redis.call('MGET', unpack(blockKeys))
		for i = 1, #blockKeys do
			local v = bgot[i]
			if v == false then
				needLock[#needLock + 1] = blockTags[i]
			elseif v ~= blockReadyValue then
				busyRaceTags[blockTags[i]] = true
				deferPartition(partition)
				raceSkips = raceSkips + 1
				if raceSkips >= maxRaceSkips then
					raceBudgetExhausted = true
				end
				return false, nil
			end
			-- v == ready: gate open, без лока
		end
	end

	local sharedPairs = {}
	if #sharedKeys > 0 then
		local got = redis.call('MGET', unpack(sharedKeys))
		for i = 1, #sharedKeys do
			if got[i] ~= false then
				sharedPairs[#sharedPairs + 1] = sharedIds[i]
				sharedPairs[#sharedPairs + 1] = got[i]
			else
				needLock[#needLock + 1] = raceSharedPrefix .. sharedIds[i]
			end
		end
	end

	if #needLock == 0 then
		if #sharedPairs == 0 then
			return true, nil
		end
		return true, sharedPairs
	end

	ensureRaceLocks()
	for i = 1, #needLock do
		local t = needLock[i]
		if raceLocks[t] then
			busyRaceTags[t] = true
			deferPartition(partition)
			raceSkips = raceSkips + 1
			if raceSkips >= maxRaceSkips then
				raceBudgetExhausted = true
			end
			return false, nil
		end
	end
	for i = 1, #needLock do
		local t = needLock[i]
		redis.call('HSET', raceLocksKey, t, taskId)
		raceLocks[t] = taskId
	end
	if #sharedPairs == 0 then
		return true, nil
	end
	return true, sharedPairs
end

-- Регистрируем/обновляем консьюмера
redis.call('SADD', consumersKey, consumerId)
redis.call('SET', consumerKey, currentUnix, 'EX', 120)

local results = {}

-- blocked: ZSET member=partition (ordered Reject). Группы — score в groups.
-- Cleanup expired — в ping.
local blockedKey = "queue:" .. queueName .. ":blocked"
local groupsKey = "queue:" .. queueName .. ":groups"

local activeBlocked = redis.call('ZRANGEBYSCORE', blockedKey, '(' .. now, '+inf')
local blockedParts = {}
for bi = 1, #activeBlocked do
	blockedParts[activeBlocked[bi]] = true
end

-- Активные блоки групп (score > now) — один раз в память, не ZSCORE на каждую партицию.
local blockedGroupsList = redis.call('ZRANGEBYSCORE', groupsKey, '(' .. now, '+inf')
local blockedGroups = {}
for gi = 1, #blockedGroupsList do
	blockedGroups[blockedGroupsList[gi]] = true
end

math.randomseed((currentUnix % 100000) * 1000000 + tonumber(currentTime[2]))

-- F4 algorithms (из github.com/Rinsvent/rate-limiter). Все возвращают waitMs.
local function rlTokenBucket(bucketKey, r, expireSec)
	local data = redis.call('HMGET', bucketKey, 'tokens', 'lastRefill')
	local tokens = r.value
	local lastRefill = nowSec
	if data[1] ~= false then tokens = tonumber(data[1]) end
	if data[2] ~= false then lastRefill = tonumber(data[2]) end
	local elapsed = nowSec - lastRefill
	if elapsed < 0 then elapsed = 0 end
	tokens = math.min(r.value, tokens + r.refill * elapsed)
	if tokens < 1 then
		local waitMs = math.ceil((1 - tokens) / r.refill * 1000)
		if waitMs < 1 then waitMs = 1 end
		return 0, waitMs
	end
	tokens = tokens - 1
	redis.call('HSET', bucketKey, 'tokens', tostring(tokens), 'lastRefill', tostring(nowSec))
	redis.call('EXPIRE', bucketKey, expireSec)
	return 1, 0
end

local function rlLeakyBucket(bucketKey, r, expireSec)
	local data = redis.call('HMGET', bucketKey, 'level', 'lastLeak')
	local level = 0
	local lastLeak = nowSec
	if data[1] ~= false then level = tonumber(data[1]) end
	if data[2] ~= false then lastLeak = tonumber(data[2]) end
	local elapsed = nowSec - lastLeak
	if elapsed < 0 then elapsed = 0 end
	level = math.max(0, level - math.floor(r.refill * elapsed))
	if level >= r.value then
		local waitMs = math.ceil((level - r.value + 1) / r.refill * 1000)
		if waitMs < 1 then waitMs = 1 end
		return 0, waitMs
	end
	level = level + 1
	redis.call('HSET', bucketKey, 'level', tostring(level), 'lastLeak', tostring(nowSec))
	redis.call('EXPIRE', bucketKey, expireSec)
	return 1, 0
end

local function rlFixedWindow(bucketKey, r, expireSec)
	local windowNum = math.floor(nowSec / r.durSec)
	local bkey = bucketKey .. ":" .. tostring(windowNum)
	local count = redis.call('GET', bkey)
	count = count and tonumber(count) or 0
	if count >= r.value then
		local nextWindow = (windowNum + 1) * r.durSec
		local waitMs = math.ceil((nextWindow - nowSec) * 1000)
		if waitMs < 1 then waitMs = 1 end
		return 0, waitMs
	end
	redis.call('INCR', bkey)
	redis.call('EXPIRE', bkey, expireSec)
	return 1, 0
end

local function rlSlidingWindowLog(bucketKey, r, expireSec)
	local cutoff = now - r.durMs
	redis.call('ZREMRANGEBYSCORE', bucketKey, '0', tostring(cutoff))
	local count = redis.call('ZCARD', bucketKey)
	if count >= r.value then
		local oldest = redis.call('ZRANGE', bucketKey, 0, 0, 'WITHSCORES')
		local waitMs = r.durMs
		if #oldest >= 2 then
			local w = r.durMs - (now - tonumber(oldest[2]))
			if w > 0 then waitMs = math.ceil(w) end
		end
		if waitMs < 1 then waitMs = 1 end
		return 0, waitMs
	end
	local member = tostring(now) .. "-" .. tostring(math.random(1, 1000000000))
	redis.call('ZADD', bucketKey, now, member)
	redis.call('EXPIRE', bucketKey, expireSec)
	return 1, 0
end

local function rlSlidingWindowCounter(bucketKey, r, expireSec)
	local window_duration = r.durSec
	local current_window = math.floor(nowSec / window_duration)
	local window_start = current_window * window_duration
	local elapsed = nowSec - window_start
	local prev_window_num = current_window - 1
	local curr = redis.call('HGET', bucketKey, 'curr')
	local count = redis.call('HGET', bucketKey, 'count')
	local prev = redis.call('HGET', bucketKey, 'prev')
	local prev_count = redis.call('HGET', bucketKey, 'prev_count')
	curr = curr and tonumber(curr) or current_window
	count = count and tonumber(count) or 0
	prev = prev and tonumber(prev) or prev_window_num
	prev_count = prev_count and tonumber(prev_count) or 0
	if curr < current_window then
		if current_window - curr == 1 then
			prev = curr
			prev_count = count
		else
			prev = current_window - 1
			prev_count = 0
		end
		curr = current_window
		count = 0
	end
	local weighted = prev_count * (1 - elapsed / window_duration) + count
	if weighted >= r.value then
		local waitSec = window_duration - elapsed
		if waitSec < 0 then waitSec = window_duration end
		local waitMs = math.ceil(waitSec * 1000)
		if waitMs < 1 then waitMs = 1 end
		return 0, waitMs
	end
	count = count + 1
	redis.call('HSET', bucketKey, 'curr', curr, 'count', count, 'prev', prev, 'prev_count', prev_count)
	redis.call('EXPIRE', bucketKey, math.max(expireSec, math.ceil(window_duration * 3)))
	return 1, 0
end

-- keyPrefix уже включает scope (g:name / p:name); в ключ добавляем algo + окно
local function rlCheckRates(keyPrefix, rates)
	local algo = rlAlgo
	if algo == "" then
		algo = "token-bucket"
	end
	for i = 1, #rates do
		local r = rates[i]
		local expireSec = math.max(1, math.ceil(r.durSec) * 2)
		local base = keyPrefix .. ":" .. algo .. ":" .. tostring(r.value) .. ":" .. tostring(r.durMs)
		local allowed, waitMs = 1, 0
		if algo == "token-bucket" then
			allowed, waitMs = rlTokenBucket(base, r, expireSec)
		elseif algo == "leaky-bucket" then
			allowed, waitMs = rlLeakyBucket(base, r, expireSec)
		elseif algo == "fixed-window-counter" then
			allowed, waitMs = rlFixedWindow(base, r, expireSec)
		elseif algo == "sliding-window-log" then
			allowed, waitMs = rlSlidingWindowLog(base, r, expireSec)
		elseif algo == "sliding-window-counter" then
			allowed, waitMs = rlSlidingWindowCounter(base, r, expireSec)
		else
			allowed, waitMs = rlTokenBucket(base, r, expireSec)
		end
		if allowed == 0 then
			return false, waitMs
		end
	end
	return true, 0
end

-- Admission: partition в blockedParts, или любая группа в blockedGroups
local function isAdmissionBlocked(partition)
	if blockedParts[partition] then
		return true
	end
	local pgroups = redis.call('SMEMBERS', "queue:" .. queueName .. ":partition:" .. partition .. ":groups")
	if #pgroups == 0 then
		return blockedGroups["_default"] == true
	end
	for gi = 1, #pgroups do
		if blockedGroups[pgroups[gi]] then
			return true
		end
	end
	return false
end

local function groupReadyAnywhere(g)
	if redis.call('ZCARD', "queue:" .. queueName .. ":group:" .. g .. ":ready") > 0 then
		return true
	end
	for qi = 1, #queuePriorities do
		local qp = tonumber(queuePriorities[qi]) or 0
		if qp > 0 then
			if redis.call('ZCARD', "queue:" .. queueName .. ":" .. qpPrefix(qp) .. "group:" .. g .. ":ready") > 0 then
				return true
			end
		end
	end
	return false
end

local function partitionInAnyLane(partition)
	if redis.call('SISMEMBER', "queue:" .. queueName .. ":partitions", partition) == 1 then
		return true
	end
	for qi = 1, #queuePriorities do
		local qp = tonumber(queuePriorities[qi]) or 0
		if qp > 0 then
			if redis.call('SISMEMBER', "queue:" .. queueName .. ":" .. qpPrefix(qp) .. "partitions", partition) == 1 then
				return true
			end
		end
	end
	return false
end

local function removePartitionFromIndexes(partition)
	local partitionsKey = "queue:" .. queueName .. ":" .. currentPref .. "partitions"
	redis.call('SREM', partitionsKey, partition)
	local pgroups = redis.call('SMEMBERS', "queue:" .. queueName .. ":partition:" .. partition .. ":groups")
	for gi = 1, #pgroups do
		local g = pgroups[gi]
		local readyKey = "queue:" .. queueName .. ":" .. currentPref .. "group:" .. g .. ":ready"
		redis.call('ZREM', readyKey, partition)
		if redis.call('ZCARD', readyKey) == 0 then
			redis.call('DEL', readyKey)
			if not groupReadyAnywhere(g) then
				local gs = redis.call('ZSCORE', groupsKey, g)
				if (not gs) or (tonumber(gs) <= now) then
					redis.call('ZREM', groupsKey, g)
				end
			end
		end
	end
	local partGroupsKey = "queue:" .. queueName .. ":partition:" .. partition .. ":groups"
	local partPrioKey = "queue:" .. queueName .. ":" .. currentPref .. "partition:" .. partition .. ":priorities"
	if not partitionInAnyLane(partition) then
		redis.call('DEL', partGroupsKey, partPrioKey)
	else
		redis.call('DEL', partPrioKey)
	end
end

-- Функция для получения одной задачи из партиции
-- Партиции с префиксом "!" — эксклюзивный lock на консьюмера.
-- Rate-limit блоки (blocked / groups) проверяются в isAdmissionBlocked до вызова.
-- F4 RL списывается только перед ZREM готовой задачи (не на locked/пустой партиции).
local function getFromPartition(partition, group)
    local needsLock = partition:sub(1, 1) == "!"
    
    if needsLock then
        local partitionLockKey = "queue:" .. queueName .. ":partition:" .. partition .. ":lock"
        local lockOwner = redis.call('GET', partitionLockKey)
        if lockOwner == false then
            local lockAcquired = redis.call('SET', partitionLockKey, consumerId, 'NX')
            if not lockAcquired then
                return nil
            end
            acquiredPartitionLocks[partition] = true
        elseif lockOwner ~= consumerId then
            if checkDeadConsumerLocks and redis.call('SISMEMBER', consumersKey, lockOwner) == 0 then
                -- stale-lock: владельца нет в consumers — забираем и продолжаем Get.
                redis.call('DEL', partitionLockKey)
                local lockAcquired = redis.call('SET', partitionLockKey, consumerId, 'NX')
                if not lockAcquired then
                    return nil
                end
                acquiredPartitionLocks[partition] = true
            else
                return nil
            end
        end
    end

	-- :count только для ordered (!): нужен, чтобы снять :lock при prefetch>1
	local consumerPartitionCountKey = "queue:" .. queueName .. ":consumer:" .. consumerId .. ":partition:" .. partition .. ":count"
	local found = false
	local prioritiesKey = "queue:" .. queueName .. ":" .. currentPref .. "partition:" .. partition .. ":priorities"
	local priorities = redis.call('ZREVRANGE', prioritiesKey, 0, -1)
	for p = 1, #priorities do
		local maxPriority = priorities[p]
		local partitionQueueKey = "queue:" .. queueName .. ":" .. currentPref .. "partition:" .. partition .. ":" .. maxPriority
		local tasks = redis.call('ZRANGE', partitionQueueKey, 0, 0, 'WITHSCORES')

		if #tasks >= 2 then
			found = true
			local taskId = tasks[1]
			local taskScore = tonumber(tasks[2])
			if taskScore <= now then
				-- F4: group затем partition; deny → block scope, задачу не берём
				if #gRates > 0 then
					local ok, waitMs = rlCheckRates("queue:" .. queueName .. ":rl:g:" .. group, gRates)
					if not ok then
						local unlockAt = now + waitMs
						redis.call('ZADD', groupsKey, unlockAt, group)
						blockedGroups[group] = true
						return nil
					end
				end
				if #pRates > 0 then
					local ok, waitMs = rlCheckRates("queue:" .. queueName .. ":rl:p:" .. partition, pRates)
					if not ok then
						local unlockAt = now + waitMs
						redis.call('ZADD', blockedKey, unlockAt, partition)
						blockedParts[partition] = true
						return nil
					end
				end
				-- F5: race! / race!s! locks (opt-in) до ZREM
				local raceOk, sharedPairs = tryAcquireRace(taskId, partition)
				if not raceOk then
					return nil
				end
				redis.call('ZREM', partitionQueueKey, taskId)
				-- value = original ZSET score (Reject/ping reclaim без priority+1)
				redis.call('HSET', consumerTasksKey, taskId, tostring(taskScore))
				if needsLock then
					redis.call('INCR', consumerPartitionCountKey)
				end
				partitionHadTask[partition] = true
				local payloadKey = "queue:" .. queueName .. ":payload:" .. taskId
				local rejectCountKey = "queue:" .. queueName .. ":reject_count:" .. taskId
				local got = redis.call('MGET', payloadKey, rejectCountKey)
				local payload = got[1]
				if payload == false then
					payload = ""
				end
				local rejectCount = tonumber(got[2]) or 0
				if sharedPairs == nil then
					sharedPairs = {}
				end
				-- maxPriority / currentPref уже известны — без лишних redis.call
				-- (Tags/Groups: SMEMBERS дорого на prefetch → не отдаём)
				local qpNum = 0
				if currentPref ~= "" then
					qpNum = tonumber(currentPref:match("^qp(%d+):")) or 0
				end
				-- taskScore — исходный ZSET score (Scheduled ms) для Go Task.Scheduled
				return {taskId, partition, payload, tostring(rejectCount), tostring(taskScore), sharedPairs, tostring(maxPriority), tostring(qpNum)}
			end
		else
			redis.call('ZREM', prioritiesKey, maxPriority)
		end
	end

	-- дошли сюда без return: либо пусто, либо только future head(s)
	if found then
		deferPartition(partition)
	else
		removePartitionFromIndexes(partition)
	end
    
    return nil
end

-- group — текущая группа обхода (из tryReadyGroup); RL group-слой считает/блокирует именно её
local function tryCollectFromPartition(partition, group)
	if isPartitionDeferred(partition) then
		return nil
	end
	if isAdmissionBlocked(partition) then
		return nil
	end
	return getFromPartition(partition, group)
end

-- Группа, из которой взята последняя задача collectOne (нужна добору samePartition для RL group-слоя).
local lastCollectedGroup = nil

-- ready = ZSET (score=0). Random start index → ZRANGE батчами по кругу. Память O(batch).
local function tryReadyGroup(g)
	local readyKey = "queue:" .. queueName .. ":" .. currentPref .. "group:" .. g .. ":ready"
	local n = redis.call('ZCARD', readyKey)
	if n == 0 then
		return nil
	end
	local start = math.random(0, n - 1)
	local seen = 0
	while seen < n do
		local from = (start + seen) % n
		local take = math.min(32, n - seen)
		local parts
		if from + take <= n then
			parts = redis.call('ZRANGE', readyKey, from, from + take - 1)
		else
			local left = n - from
			parts = redis.call('ZRANGE', readyKey, from, n - 1)
			local rest = redis.call('ZRANGE', readyKey, 0, take - left - 1)
			for ri = 1, #rest do
				parts[#parts + 1] = rest[ri]
			end
		end
		for pi = 1, #parts do
			local taskData = tryCollectFromPartition(parts[pi], g)
			if taskData then
				lastCollectedGroup = g
				return taskData
			end
		end
		seen = seen + take
	end
	return nil
end

-- Обход свободных групп в текущем currentPref.
local function collectOneLane()
	for _ = 1, 64 do
		local n = redis.call('ZCOUNT', groupsKey, '-inf', now)
		if n == 0 then
			return nil
		end
		local start = math.random(0, n - 1)
		local seen = 0
		local refresh = false
		while seen < n do
			local from = (start + seen) % n
			local take = math.min(32, n - seen)
			local batch
			if from + take <= n then
				batch = redis.call('ZRANGEBYSCORE', groupsKey, '-inf', now, 'LIMIT', from, take)
			else
				local left = n - from
				batch = redis.call('ZRANGEBYSCORE', groupsKey, '-inf', now, 'LIMIT', from, left)
				local rest = redis.call('ZRANGEBYSCORE', groupsKey, '-inf', now, 'LIMIT', 0, take - left)
				for ri = 1, #rest do
					batch[#batch + 1] = rest[ri]
				end
			end
			for bi = 1, #batch do
				local g = batch[bi]
				if not blockedGroups[g] then
					local taskData = tryReadyGroup(g)
					if taskData then
						return taskData
					end
					if blockedGroups[g] then
						refresh = true
						break
					end
				end
			end
			if refresh then
				break
			end
			seen = seen + take
		end
		if not refresh then
			return nil
		end
	end
	return nil
end

-- Сначала qp>0 (больший раньше, как PartitionPriority), затем legacy.
local function collectOne()
	for qi = 1, #queuePriorities do
		local qp = tonumber(queuePriorities[qi]) or 0
		if qp > 0 and (skipQueuePriorityAtOrAbove == nil or qp < skipQueuePriorityAtOrAbove) then
			currentPref = qpPrefix(qp)
			local taskData = collectOneLane()
			if taskData then
				return taskData
			end
			skipQueuePriorityAtOrAbove = qp
			if redis.call('SCARD', "queue:" .. queueName .. ":" .. currentPref .. "partitions") == 0 then
				redis.call('ZREM', queuePrioritiesKey, tostring(qp))
			end
		end
	end
	currentPref = ""
	return collectOneLane()
end

-- Основной цикл: prefetch; samePartition — та же партиция и тот же qp.
local stickyPartition = nil
local stickyGroup = nil
local stickyPref = nil
while #results / 8 < prefetchCount do
	if raceBudgetExhausted then
		break
	end
	local taskData
	if stickyPartition then
		currentPref = stickyPref or ""
		taskData = tryCollectFromPartition(stickyPartition, stickyGroup)
	else
		taskData = collectOne()
	end
	if not taskData then
		break
	end
	if samePartition and not stickyPartition then
		stickyPartition = taskData[2]
		stickyGroup = lastCollectedGroup
		stickyPref = currentPref
	end
	results[#results + 1] = taskData[1]
	results[#results + 1] = taskData[2]
	results[#results + 1] = taskData[3]
	results[#results + 1] = taskData[4]
	results[#results + 1] = taskData[5] -- Scheduled ms (taskScore)
	local sharedPairs = taskData[6]
	if sharedPairs == nil then
		sharedPairs = {}
	end
	results[#results + 1] = sharedPairs
	results[#results + 1] = taskData[7] or "0" -- PartitionPriority
	results[#results + 1] = taskData[8] or "0" -- QueuePriority
end

-- Если мы залочили партицию в рамках этого GET, но не взяли из неё ни одной задачи,
-- снимаем лок. Повторный GET тем же consumer случится только после обработки пачки, т.е. “хвостов” не остаётся.
for partition, _ in pairs(acquiredPartitionLocks) do
	if partitionHadTask[partition] ~= true then
		local partitionLockKey = "queue:" .. queueName .. ":partition:" .. partition .. ":lock"
		redis.call('DEL', partitionLockKey)
	end
end

return results
`)

// getAckScript возвращает Lua скрипт для подтверждения задачи
// ARGV[1] = queue name
// ARGV[2] = task ID
// ARGV[3] = consumer ID
// ARGV[4] = idempotencyTtl seconds
// ARGV[5] = concurrencyTags ("1" / "0") — F5 release race locks
// ARGV[6] = sharedTTL seconds
// ARGV[7] = nShared
// ARGV[8..] = id, blob pairs; затем nOpen, openId...
var ackScript = redis.NewScript(`
local queueName = ARGV[1]
local taskId = ARGV[2]
local consumerId = ARGV[3]
local idempotencyTtl = tonumber(ARGV[4])
local concurrencyTags = (ARGV[5] or "") == "1"
local sharedTtl = tonumber(ARGV[6]) or 86400
local nShared = tonumber(ARGV[7]) or 0
if sharedTtl < 1 then
	sharedTtl = 86400
end
local argi = 8

-- Формируем ключи
local payloadKey = "queue:" .. queueName .. ":payload:" .. taskId
local partitionKey = "queue:" .. queueName .. ":partition:" .. taskId
local priorityKey = "queue:" .. queueName .. ":priority:" .. taskId
local debPrefix = "deb!"
local isDebounce = taskId:sub(1, #debPrefix) == debPrefix
local raceLocksKey = "queue:" .. queueName .. ":race:locks"
local raceTagsKey = "queue:" .. queueName .. ":race:tags:" .. taskId
local raceSharedPrefix = "race!s!"
local raceBlockPrefix = "race!b!"

-- F5: снять локи только если consumer включил concurrency (acquire был только тогда)
local function releaseRaceLocks()
	if not concurrencyTags then
		return
	end
	local tags = redis.call('SMEMBERS', raceTagsKey)
	if #tags == 0 then
		return
	end
	local toDel = {}
	for i = 1, #tags do
		if redis.call('HGET', raceLocksKey, tags[i]) == taskId then
			toDel[#toDel + 1] = tags[i]
		end
	end
	if #toDel > 0 then
		redis.call('HDEL', raceLocksKey, unpack(toDel))
	end
end

-- race!s! Shared: SET EX (overwrite) и снять лок этого id
local function applyShared()
	if nShared <= 0 then
		return
	end
	local toDel = {}
	for _ = 1, nShared do
		local id = ARGV[argi]
		local blob = ARGV[argi + 1]
		argi = argi + 2
		if id ~= nil and id ~= "" and blob ~= nil then
			redis.call('SET', "queue:" .. queueName .. ":shared:" .. id, blob, 'EX', math.floor(sharedTtl))
			if concurrencyTags then
				local tag = raceSharedPrefix .. id
				if redis.call('HGET', raceLocksKey, tag) == taskId then
					toDel[#toDel + 1] = tag
				end
			end
		end
	end
	if #toDel > 0 then
		redis.call('HDEL', raceLocksKey, unpack(toDel))
	end
end

-- race!b! open: SET block:{id} ready + unlock
local function applyOpenBlocks()
	local nOpen = tonumber(ARGV[argi]) or 0
	argi = argi + 1
	if nOpen <= 0 then
		return
	end
	local toDel = {}
	for _ = 1, nOpen do
		local id = ARGV[argi]
		argi = argi + 1
		if id ~= nil and id ~= "" then
			redis.call('SET', "queue:" .. queueName .. ":block:" .. id, "ready", 'EX', math.floor(sharedTtl))
			if concurrencyTags then
				local tag = raceBlockPrefix .. id
				if redis.call('HGET', raceLocksKey, tag) == taskId then
					toDel[#toDel + 1] = tag
				end
			end
		end
	end
	if #toDel > 0 then
		redis.call('HDEL', raceLocksKey, unpack(toDel))
	end
end

-- Проверяем что задача в processing и принадлежит этому консьюмеру
local consumerTasksKey = "queue:" .. queueName .. ":consumer:" .. consumerId .. ":tasks"

local processingAt = redis.call('HGET', consumerTasksKey, taskId)
if processingAt == false then
    return 0
end

-- Партиция нужна для :count/:lock (как раньше); priority — только на F2-reactivate
local partitionCode = redis.call('GET', partitionKey) or "base"

-- :count / :lock только для ordered (!)
if partitionCode:sub(1, 1) == "!" then
	local consumerPartitionCountKey = "queue:" .. queueName .. ":consumer:" .. consumerId .. ":partition:" .. partitionCode .. ":count"
	local consumerPartitionCount = redis.call('DECR', consumerPartitionCountKey) or 0
	if consumerPartitionCount <= 0 then
		local partitionLockKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":lock"
		local lockOwner = redis.call('GET', partitionLockKey)
		if lockOwner == consumerId then
			redis.call('DEL', consumerPartitionCountKey, partitionLockKey)
		else
			redis.call('DEL', consumerPartitionCountKey)
		end
	end
end

redis.call('HDEL', consumerTasksKey, taskId)
applyShared()
applyOpenBlocks()
releaseRaceLocks()

-- F2: только deb! id — MGET флага/priority/qpriority и reactivate; иначе нулевой оверхед
if isDebounce then
	local debounceKey = "queue:" .. queueName .. ":debounce:" .. taskId
	local qpriorityKey = "queue:" .. queueName .. ":qpriority:" .. taskId
	local debMeta = redis.call('MGET', debounceKey, priorityKey, qpriorityKey)
	local debounceUntil = debMeta[1]
	if debounceUntil ~= false then
		local priority = tonumber(debMeta[2]) or 0
		local queuePriority = 0
		if debMeta[3] ~= false then
			queuePriority = tonumber(debMeta[3]) or 0
		end
		local pref = ""
		if queuePriority > 0 then
			pref = "qp" .. tostring(queuePriority) .. ":"
			redis.call('ZADD', "queue:" .. queueName .. ":priorities", queuePriority, tostring(queuePriority))
		end
		local score = tonumber(debounceUntil) or 0
		local partitionQueueKey = "queue:" .. queueName .. ":" .. pref .. "partition:" .. partitionCode .. ":" .. tostring(priority)
		redis.call('ZADD', partitionQueueKey, score, taskId)
		redis.call('SADD', "queue:" .. queueName .. ":" .. pref .. "partitions", partitionCode)
		redis.call('ZADD', "queue:" .. queueName .. ":" .. pref .. "partition:" .. partitionCode .. ":priorities", priority, tostring(priority))
		local groupsKey = "queue:" .. queueName .. ":groups"
		local pgroups = redis.call('SMEMBERS', "queue:" .. queueName .. ":partition:" .. partitionCode .. ":groups")
		if #pgroups == 0 then
			pgroups = {"_default"}
			redis.call('SADD', "queue:" .. queueName .. ":partition:" .. partitionCode .. ":groups", "_default")
		end
		for gi = 1, #pgroups do
			local g = pgroups[gi]
			if not redis.call('ZSCORE', groupsKey, g) then
				redis.call('ZADD', groupsKey, 0, g)
			end
			redis.call('ZADD', "queue:" .. queueName .. ":" .. pref .. "group:" .. g .. ":ready", 0, partitionCode)
		end
		redis.call('DEL', debounceKey)
		-- race:tags оставляем — задача снова в очереди (total не меняется)
		return 1
	end
end

-- Удаляем meta; payload либо TTL-маркер идемпотентности, либо тоже в DEL
local keysToDelete = {
	partitionKey,
	priorityKey,
	"queue:" .. queueName .. ":qpriority:" .. taskId,
	"queue:" .. queueName .. ":reject_count:" .. taskId,
	"queue:" .. queueName .. ":step:" .. taskId,
	"queue:" .. queueName .. ":headers:" .. taskId,
	raceTagsKey,
}
if idempotencyTtl > 0 then
	redis.call('SET', payloadKey, '0', 'EX', math.floor(idempotencyTtl))
else
	keysToDelete[#keysToDelete + 1] = payloadKey
end
redis.call('DEL', unpack(keysToDelete))

-- Чистим индексы тегов (если были); пустой SET тега удаляем явно
local tagsKey = "queue:" .. queueName .. ":tags:" .. taskId
local tags = redis.call('SMEMBERS', tagsKey)
for t = 1, #tags do
	local tagKey = "queue:" .. queueName .. ":tag:" .. tags[t]
	redis.call('SREM', tagKey, taskId)
	if redis.call('SCARD', tagKey) == 0 then
		redis.call('DEL', tagKey)
	end
end
redis.call('DEL', tagsKey)

do
	local _qd = tonumber(redis.call('ZINCRBY', 'srq:queues', -1, queueName))
	if _qd < 0 then
		redis.call('ZADD', 'srq:queues', 0, queueName)
	end
end

return 1
`)

// getRejectScript возвращает Lua скрипт для отклонения задачи.
// Возврат в тот же priority с originalScore из consumer:tasks.
// deadLetter=1 → ZADD queue:{q}:dlq (score=now), meta не чистим; srq:queues −1.
// dlqMax>0 → после ZADD вытеснить самые старые (meta purge), ≤dlqMaxTrimPerReject за вызов.
// При waitTime > 0: ordered → blocked; группы → score unlockAt в groups.
// ARGV[1]=queue ARGV[2]=taskId ARGV[3]=consumerId ARGV[4]=waitTime
// ARGV[5]=blockGroups ARGV[6]=concurrencyTags ARGV[7]=extendGroupBlocksOnly
// ARGV[8]=blockTags ARGV[9]=deadLetter ("1"/"0") ARGV[10]=dlqMax (0=без лимита)
var rejectScript = redis.NewScript(`
local queueName = ARGV[1]
local taskId = ARGV[2]
local consumerId = ARGV[3]
local waitTime = tonumber(ARGV[4]) or 0
local blockGroupsStr = ARGV[5] or ""
local concurrencyTags = (ARGV[6] or "") == "1"
local extendGroupBlocksOnly = (ARGV[7] or "") == "1"
local blockTagsStr = ARGV[8] or ""
local deadLetter = (ARGV[9] or "") == "1"
local dlqMax = tonumber(ARGV[10]) or 0
local dlqMaxTrimPerReject = 32

local consumerTasksKey = "queue:" .. queueName .. ":consumer:" .. consumerId .. ":tasks"

-- Проверяем что задача в processing и принадлежит этому консьюмеру
local meta = redis.call('HGET', consumerTasksKey, taskId)
if meta == false then
    return 0
end

-- Получаем партицию / partitionPriority / queuePriority одним MGET
local partitionKey = "queue:" .. queueName .. ":partition:" .. taskId
local priorityKey = "queue:" .. queueName .. ":priority:" .. taskId
local qpriorityKey = "queue:" .. queueName .. ":qpriority:" .. taskId
local taskMeta = redis.call('MGET', partitionKey, priorityKey, qpriorityKey)
local partitionCode = taskMeta[1] or "base"
if partitionCode == false then
	partitionCode = "base"
end
local priority = tonumber(taskMeta[2]) or 0
local queuePriority = 0
if taskMeta[3] ~= false then
	queuePriority = tonumber(taskMeta[3]) or 0
end
local pref = ""
if queuePriority > 0 then
	pref = "qp" .. tostring(queuePriority) .. ":"
end

local now = redis.call('TIME')
local nowMs = now[1] * 1000 + math.floor(now[2] / 1000)
local blockNowMs = nowMs

-- originalScore в meta; если нет/битое — nowMs
local score = tonumber(meta) or nowMs

local needsLockUnlock = partitionCode:sub(1, 1) == "!"

-- Инкрементируем счётчик reject для расчёта задержки при следующих попытках
local rejectCountKey = "queue:" .. queueName .. ":reject_count:" .. taskId
redis.call('INCR', rejectCountKey)

local groupsKey = "queue:" .. queueName .. ":groups"

if deadLetter then
	-- DLQ: единый ZSET, meta (payload/partition/…) оставляем для RetryDLQ
	local dlqKey = "queue:" .. queueName .. ":dlq"
	redis.call('SET', "queue:" .. queueName .. ":dlq:score:" .. taskId, tostring(score))
	redis.call('ZADD', dlqKey, nowMs, taskId)
	local _qd = tonumber(redis.call('ZINCRBY', 'srq:queues', -1, queueName))
	if _qd < 0 then
		redis.call('ZADD', 'srq:queues', 0, queueName)
	end
	-- снять из tag: индексов (backpressure); tags:{id} оставить для RetryDLQ
	do
		local tagsKey = "queue:" .. queueName .. ":tags:" .. taskId
		local tags = redis.call('SMEMBERS', tagsKey)
		for ti = 1, #tags do
			local tagKey = "queue:" .. queueName .. ":tag:" .. tags[ti]
			redis.call('SREM', tagKey, taskId)
			if redis.call('SCARD', tagKey) == 0 then
				redis.call('DEL', tagKey)
			end
		end
	end
	-- ёмкость: вытеснить oldest (не трогаем только что добавленный — у него max score)
	if dlqMax > 0 then
		local card = redis.call('ZCARD', dlqKey)
		local trimmed = 0
		while card > dlqMax and trimmed < dlqMaxTrimPerReject do
			local oldest = redis.call('ZRANGE', dlqKey, 0, 0)
			if #oldest == 0 then
				break
			end
			local oid = oldest[1]
			if oid == taskId then
				break
			end
			redis.call('ZREM', dlqKey, oid)
			local tagsKey = "queue:" .. queueName .. ":tags:" .. oid
			redis.call('DEL',
				"queue:" .. queueName .. ":dlq:score:" .. oid,
				"queue:" .. queueName .. ":payload:" .. oid,
				"queue:" .. queueName .. ":partition:" .. oid,
				"queue:" .. queueName .. ":priority:" .. oid,
				"queue:" .. queueName .. ":qpriority:" .. oid,
				"queue:" .. queueName .. ":reject_count:" .. oid,
				"queue:" .. queueName .. ":step:" .. oid,
				"queue:" .. queueName .. ":headers:" .. oid,
				"queue:" .. queueName .. ":race:tags:" .. oid,
				"queue:" .. queueName .. ":debounce:" .. oid,
				tagsKey)
			card = card - 1
			trimmed = trimmed + 1
		end
	end
else
	-- Возвращаем задачу обратно в очередь (тот же qp + partitionPriority + исходный score)
	local partitionQueueKey = "queue:" .. queueName .. ":" .. pref .. "partition:" .. partitionCode .. ":" .. priority
	redis.call('ZADD', partitionQueueKey, score, taskId)
	local partitionsKey = "queue:" .. queueName .. ":" .. pref .. "partitions"
	redis.call('SADD', partitionsKey, partitionCode)
	local prioritiesKey = "queue:" .. queueName .. ":" .. pref .. "partition:" .. partitionCode .. ":priorities"
	redis.call('ZADD', prioritiesKey, priority, tostring(priority))
	if queuePriority > 0 then
		redis.call('ZADD', "queue:" .. queueName .. ":priorities", queuePriority, tostring(queuePriority))
	end

	-- ready = «есть/была работа». Блок группы — score в groups (не трогаем, если уже > now).
	local pgroups = redis.call('SMEMBERS', "queue:" .. queueName .. ":partition:" .. partitionCode .. ":groups")
	if #pgroups == 0 then
		pgroups = {"_default"}
		redis.call('SADD', "queue:" .. queueName .. ":partition:" .. partitionCode .. ":groups", "_default")
	end
	for gi = 1, #pgroups do
		local g = pgroups[gi]
		if not redis.call('ZSCORE', groupsKey, g) then
			redis.call('ZADD', groupsKey, 0, g)
		end
		redis.call('ZADD', "queue:" .. queueName .. ":" .. pref .. "group:" .. g .. ":ready", 0, partitionCode)
	end
end

-- :count / :lock только для ordered (!)
if needsLockUnlock then
	local consumerPartitionCountKey = "queue:" .. queueName .. ":consumer:" .. consumerId .. ":partition:" .. partitionCode .. ":count"
	local consumerPartitionCount = redis.call('DECR', consumerPartitionCountKey) or 0
	if consumerPartitionCount <= 0 then
		local partitionLockKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":lock"
		local lockOwner = redis.call('GET', partitionLockKey)
		if lockOwner == consumerId then
			redis.call('DEL', consumerPartitionCountKey, partitionLockKey)
		else
			redis.call('DEL', consumerPartitionCountKey)
		end
	end
end

-- При waitTime > 0: ordered → blocked; группы → score в groups
if waitTime > 0 then
	local unlockAt = math.floor(blockNowMs + waitTime * 1000)
	if needsLockUnlock then
		redis.call('ZADD', "queue:" .. queueName .. ":blocked", unlockAt, partitionCode)
	end
	if blockGroupsStr ~= "" then
		for group in string.gmatch(blockGroupsStr, "[^#]+") do
			if group ~= "" then
				if extendGroupBlocksOnly then
					redis.call('ZADD', groupsKey, 'GT', unlockAt, group)
				else
					redis.call('ZADD', groupsKey, unlockAt, group)
				end
			end
		end
	end
end

-- race!b! park: SET block:{id} wait + TTL (до release locks)
if blockTagsStr ~= "" and waitTime > 0 then
	local ttl = math.ceil(waitTime)
	if ttl < 1 then
		ttl = 1
	end
	for id in string.gmatch(blockTagsStr, "[^#]+") do
		if id ~= "" then
			redis.call('SET', "queue:" .. queueName .. ":block:" .. id, "1", 'EX', ttl)
		end
	end
end

-- Удаляем задачу из hash консьюмера (prefetch)
redis.call('HDEL', consumerTasksKey, taskId)
-- F2: снять debounce-флаг (и при DLQ, и при возврате)
local debPrefix = "deb!"
if taskId:sub(1, #debPrefix) == debPrefix then
	redis.call('DEL', "queue:" .. queueName .. ":debounce:" .. taskId)
end
-- F5: снять race locks (race:tags оставляем — meta жива)
if concurrencyTags then
	local raceLocksKey = "queue:" .. queueName .. ":race:locks"
	local raceTagsKey = "queue:" .. queueName .. ":race:tags:" .. taskId
	local rtags = redis.call('SMEMBERS', raceTagsKey)
	local toDel = {}
	for i = 1, #rtags do
		if redis.call('HGET', raceLocksKey, rtags[i]) == taskId then
			toDel[#toDel + 1] = rtags[i]
		end
	end
	if #toDel > 0 then
		redis.call('HDEL', raceLocksKey, unpack(toDel))
	end
end

return 1
`)

// blockGroupsScript паркует группы без привязки к задаче (Consumer/Admin.BlockGroups).
// ARGV[1] = queue name
// ARGV[2] = waitTime секунды
// ARGV[3] = groups (g1#g2#...)
// ARGV[4] = extendOnly ("1" / "0") — пауза группы только продлевается (ZADD GT)
var blockGroupsScript = redis.NewScript(`
local queueName = ARGV[1]
local waitTime = tonumber(ARGV[2]) or 0
local groupsStr = ARGV[3] or ""
local extendOnly = (ARGV[4] or "") == "1"
if waitTime <= 0 or groupsStr == "" then
	return 0
end
local t = redis.call('TIME')
local nowMs = t[1] * 1000 + math.floor(t[2] / 1000)
local unlockAt = math.floor(nowMs + waitTime * 1000)
local groupsKey = "queue:" .. queueName .. ":groups"
local n = 0
for group in string.gmatch(groupsStr, "[^#]+") do
	if group ~= "" then
		if extendOnly then
			redis.call('ZADD', groupsKey, 'GT', unlockAt, group)
		else
			redis.call('ZADD', groupsKey, unlockAt, group)
		end
		n = n + 1
	end
end
return n
`)

// getPingScript возвращает Lua скрипт для ping консьюмера и разблокировки мертвых консьюмеров
// ARGV[1] = queue name
// ARGV[2] = consumer ID
// ARGV[3] = dlqMax (0 = не тримить DLQ; >0 = дочистка до лимита батчами)
var pingScript = redis.NewScript(`
local queueName = ARGV[1]
local consumerId = ARGV[2]
local dlqMax = tonumber(ARGV[3]) or 0

local consumersKey = "queue:" .. queueName .. ":consumers"
local consumerKey = "queue:" .. queueName .. ":consumer:" .. consumerId

-- Регистрируем консьюмера
redis.call('SADD', consumersKey, consumerId)

-- Обновляем heartbeat для текущего консьюмера
local currentTime = redis.call('TIME')
local currentUnix = currentTime[1]
local now = currentUnix * 1000 + math.floor(currentTime[2] / 1000)
redis.call('SET', consumerKey, currentUnix, 'EX', 120)

-- Пытаемся получить блокировку для разблокировки партиций (только один поток выполняет разблокировку)
local unlockLockKey = "queue:" .. queueName .. ":unlock:lock"
local lockAcquired = redis.call('SET', unlockLockKey, consumerId, 'EX', 55, 'NX')
if not lockAcquired then
    -- Другой поток уже выполняет разблокировку
    return 1
end

-- Чистка expired партиций в blocked (группы сами «отпускаются» когда score <= now)
local blockedKey = "queue:" .. queueName .. ":blocked"
local groupsKey = "queue:" .. queueName .. ":groups"
redis.call('ZREMRANGEBYSCORE', blockedKey, '-inf', now)

-- DLQ cap: дочистка батчами (лимит из ARGV consumer), если Reject не успел
if dlqMax > 0 then
	local dlqKey = "queue:" .. queueName .. ":dlq"
	local card = redis.call('ZCARD', dlqKey)
	local trimmed = 0
	local dlqMaxTrimPerPing = 64
	while card > dlqMax and trimmed < dlqMaxTrimPerPing do
		local oldest = redis.call('ZRANGE', dlqKey, 0, 0)
		if #oldest == 0 then
			break
		end
		local oid = oldest[1]
		redis.call('ZREM', dlqKey, oid)
		-- tag: индексы уже сняты при Reject→DLQ; чистим meta + tags:{id}
		redis.call('DEL',
			"queue:" .. queueName .. ":dlq:score:" .. oid,
			"queue:" .. queueName .. ":payload:" .. oid,
			"queue:" .. queueName .. ":partition:" .. oid,
			"queue:" .. queueName .. ":priority:" .. oid,
			"queue:" .. queueName .. ":qpriority:" .. oid,
			"queue:" .. queueName .. ":reject_count:" .. oid,
			"queue:" .. queueName .. ":step:" .. oid,
			"queue:" .. queueName .. ":headers:" .. oid,
			"queue:" .. queueName .. ":race:tags:" .. oid,
			"queue:" .. queueName .. ":debounce:" .. oid,
			"queue:" .. queueName .. ":tags:" .. oid)
		card = card - 1
		trimmed = trimmed + 1
	end
end

-- Получаем список всех консамеров
local consumers = redis.call('SMEMBERS', consumersKey)
for i = 1, #consumers do
    local lockOwner = consumers[i]

	-- Проверяем что консьюмер жив
	local lockOwnerKey = "queue:" .. queueName .. ":consumer:" .. lockOwner
	local lastPing = redis.call('GET', lockOwnerKey)

	if lastPing == false then
		-- Консьюмер мёртв — ZADD задач per-task; индексы partitions/priorities/ready/count — 1× на partition (R5)
		local lockOwnerTasksKey = "queue:" .. queueName .. ":consumer:" .. lockOwner .. ":tasks"
		local tasksData = redis.call('HGETALL', lockOwnerTasksKey)
		local restoredParts = {}
		local partPriorities = {}

		for j = 1, #tasksData, 2 do
			local taskToReturn = tasksData[j]
			local meta = tasksData[j + 1]
			if taskToReturn then
				local taskMeta = redis.call('MGET',
					"queue:" .. queueName .. ":partition:" .. taskToReturn,
					"queue:" .. queueName .. ":priority:" .. taskToReturn,
					"queue:" .. queueName .. ":qpriority:" .. taskToReturn)
				local taskPartition = taskMeta[1]
				if taskPartition == false then
					taskPartition = "base"
				end
				local taskPriority = tonumber(taskMeta[2]) or 0
				local taskQp = 0
				if taskMeta[3] ~= false then
					taskQp = tonumber(taskMeta[3]) or 0
				end
				local pref = ""
				if taskQp > 0 then
					pref = "qp" .. tostring(taskQp) .. ":"
				end

				local score = tonumber(meta) or now
				local partitionQueueKey = "queue:" .. queueName .. ":" .. pref .. "partition:" .. taskPartition .. ":" .. taskPriority
				redis.call('ZADD', partitionQueueKey, score, taskToReturn)
				-- F2: reclaim → DEL debounce только для deb! id
				local debPrefix = "deb!"
				if taskToReturn:sub(1, #debPrefix) == debPrefix then
					redis.call('DEL', "queue:" .. queueName .. ":debounce:" .. taskToReturn)
				end
				-- F5: reclaim всегда снимает race locks этого taskId (иначе утечка)
				local raceLocksKey = "queue:" .. queueName .. ":race:locks"
				local raceTagsKey = "queue:" .. queueName .. ":race:tags:" .. taskToReturn
				local rtags = redis.call('SMEMBERS', raceTagsKey)
				local toDel = {}
				for ri = 1, #rtags do
					if redis.call('HGET', raceLocksKey, rtags[ri]) == taskToReturn then
						toDel[#toDel + 1] = rtags[ri]
					end
				end
				if #toDel > 0 then
					redis.call('HDEL', raceLocksKey, unpack(toDel))
				end

				local laneKey = pref .. taskPartition
				restoredParts[laneKey] = {part = taskPartition, pref = pref, qp = taskQp}
				if not partPriorities[laneKey] then
					partPriorities[laneKey] = {}
				end
				partPriorities[laneKey][tostring(taskPriority)] = taskPriority
			end
		end

		for laneKey, info in pairs(restoredParts) do
			local part = info.part
			local pref = info.pref
			local qp = info.qp
			redis.call('SADD', "queue:" .. queueName .. ":" .. pref .. "partitions", part)
			if qp > 0 then
				redis.call('ZADD', "queue:" .. queueName .. ":priorities", qp, tostring(qp))
			end
			local prioritiesKey = "queue:" .. queueName .. ":" .. pref .. "partition:" .. part .. ":priorities"
			for prioStr, prioNum in pairs(partPriorities[laneKey]) do
				redis.call('ZADD', prioritiesKey, prioNum, prioStr)
			end

			local pgroups = redis.call('SMEMBERS', "queue:" .. queueName .. ":partition:" .. part .. ":groups")
			if #pgroups == 0 then
				pgroups = {"_default"}
				redis.call('SADD', "queue:" .. queueName .. ":partition:" .. part .. ":groups", "_default")
			end
			for gi = 1, #pgroups do
				local g = pgroups[gi]
				if not redis.call('ZSCORE', groupsKey, g) then
					redis.call('ZADD', groupsKey, 0, g)
				end
				redis.call('ZADD', "queue:" .. queueName .. ":" .. pref .. "group:" .. g .. ":ready", 0, part)
			end

			if part:sub(1, 1) == "!" then
				redis.call('DEL',
					"queue:" .. queueName .. ":consumer:" .. lockOwner .. ":partition:" .. part .. ":count",
					"queue:" .. queueName .. ":partition:" .. part .. ":lock")
			end
		end

		redis.call('DEL', lockOwnerTasksKey)
		redis.call('SREM', consumersKey, lockOwner)
	end
end

return 1
`)

// removeScript атомарно снимает ожидающую задачу из очереди по ID (без consumer).
// ARGV[1] = queue name
// ARGV[2] = task ID
// Возврат: {status} или {status, payload} при removed.
// status: missing | inprogress | removed
var removeScript = redis.NewScript(`
local queueName = ARGV[1]
local taskId = ARGV[2]

local payloadKey = "queue:" .. queueName .. ":payload:" .. taskId
local partitionKey = "queue:" .. queueName .. ":partition:" .. taskId
local priorityKey = "queue:" .. queueName .. ":priority:" .. taskId
local qpriorityKey = "queue:" .. queueName .. ":qpriority:" .. taskId
local tagsKey = "queue:" .. queueName .. ":tags:" .. taskId

local function qpPrefix(qp)
	qp = tonumber(qp) or 0
	if qp == 0 then return "" end
	return "qp" .. tostring(qp) .. ":"
end

local queuePrioritiesKey = "queue:" .. queueName .. ":priorities"
local function groupReadyAnywhere(g)
	if redis.call('ZCARD', "queue:" .. queueName .. ":group:" .. g .. ":ready") > 0 then
		return true
	end
	local qpris = redis.call('ZRANGE', queuePrioritiesKey, 0, -1)
	for qi = 1, #qpris do
		local qp = tonumber(qpris[qi]) or 0
		if qp > 0 then
			if redis.call('ZCARD', "queue:" .. queueName .. ":" .. qpPrefix(qp) .. "group:" .. g .. ":ready") > 0 then
				return true
			end
		end
	end
	return false
end

local function partitionInAnyLane(part)
	if redis.call('SISMEMBER', "queue:" .. queueName .. ":partitions", part) == 1 then
		return true
	end
	local qpris = redis.call('ZRANGE', queuePrioritiesKey, 0, -1)
	for qi = 1, #qpris do
		local qp = tonumber(qpris[qi]) or 0
		if qp > 0 then
			if redis.call('SISMEMBER', "queue:" .. queueName .. ":" .. qpPrefix(qp) .. "partitions", part) == 1 then
				return true
			end
		end
	end
	return false
end

local function cleanupTags()
	local tags = redis.call('SMEMBERS', tagsKey)
	for t = 1, #tags do
		local tagKey = "queue:" .. queueName .. ":tag:" .. tags[t]
		redis.call('SREM', tagKey, taskId)
		if redis.call('SCARD', tagKey) == 0 then
			redis.call('DEL', tagKey)
		end
	end
	redis.call('DEL', tagsKey)
end

local meta = redis.call('MGET', partitionKey, priorityKey, qpriorityKey)
local partition = meta[1]
if not partition then
	cleanupTags()
	return {'missing'}
end

local priority = meta[2] or '0'
if priority == false then
	priority = '0'
end
local queuePriority = 0
if meta[3] ~= false then
	queuePriority = tonumber(meta[3]) or 0
end
local pref = qpPrefix(queuePriority)
local queueKey = "queue:" .. queueName .. ":" .. pref .. "partition:" .. partition .. ":" .. priority
local dlqKey = "queue:" .. queueName .. ":dlq"

-- DLQ: не в pending ZSET; снимаем как PurgeDLQ (tag: уже пуст)
if redis.call('ZSCORE', dlqKey, taskId) then
	local payload = redis.call('GET', payloadKey) or ''
	redis.call('ZREM', dlqKey, taskId)
	redis.call('DEL', payloadKey, partitionKey, priorityKey, qpriorityKey,
		"queue:" .. queueName .. ":dlq:score:" .. taskId,
		"queue:" .. queueName .. ":reject_count:" .. taskId,
		"queue:" .. queueName .. ":step:" .. taskId,
		"queue:" .. queueName .. ":headers:" .. taskId,
		"queue:" .. queueName .. ":race:tags:" .. taskId,
		"queue:" .. queueName .. ":debounce:" .. taskId,
		tagsKey)
	return {'removed', payload}
end

if redis.call('ZREM', queueKey, taskId) == 0 then
	return {'inprogress'}
end

local payload = redis.call('GET', payloadKey) or ''
local raceTagsKey = "queue:" .. queueName .. ":race:tags:" .. taskId
local raceLocksKey = "queue:" .. queueName .. ":race:locks"
local rtags = redis.call('SMEMBERS', raceTagsKey)
local raceToDel = {}
for i = 1, #rtags do
	if redis.call('HGET', raceLocksKey, rtags[i]) == taskId then
		raceToDel[#raceToDel + 1] = rtags[i]
	end
end
if #raceToDel > 0 then
	redis.call('HDEL', raceLocksKey, unpack(raceToDel))
end
redis.call('DEL', payloadKey, partitionKey, priorityKey, qpriorityKey,
	"queue:" .. queueName .. ":reject_count:" .. taskId,
	"queue:" .. queueName .. ":step:" .. taskId,
	"queue:" .. queueName .. ":headers:" .. taskId,
	raceTagsKey)
cleanupTags()

local prioritiesKey = "queue:" .. queueName .. ":" .. pref .. "partition:" .. partition .. ":priorities"
if redis.call('ZCARD', queueKey) == 0 then
	redis.call('ZREM', prioritiesKey, priority)
	if redis.call('ZCARD', prioritiesKey) == 0 then
		redis.call('SREM', "queue:" .. queueName .. ":" .. pref .. "partitions", partition)
		local t = redis.call('TIME')
		local nowMs = t[1] * 1000 + math.floor(t[2] / 1000)
		local groupsKey = "queue:" .. queueName .. ":groups"
		local pgroups = redis.call('SMEMBERS', "queue:" .. queueName .. ":partition:" .. partition .. ":groups")
		for gi = 1, #pgroups do
			local g = pgroups[gi]
			local readyKey = "queue:" .. queueName .. ":" .. pref .. "group:" .. g .. ":ready"
			redis.call('ZREM', readyKey, partition)
			if redis.call('ZCARD', readyKey) == 0 then
				redis.call('DEL', readyKey)
				if not groupReadyAnywhere(g) then
					local gs = redis.call('ZSCORE', groupsKey, g)
					if (not gs) or (tonumber(gs) <= nowMs) then
						redis.call('ZREM', groupsKey, g)
					end
				end
			end
		end
		if not partitionInAnyLane(partition) then
			redis.call('DEL', "queue:" .. queueName .. ":partition:" .. partition .. ":groups")
		end
		if pref ~= "" and redis.call('SCARD', "queue:" .. queueName .. ":" .. pref .. "partitions") == 0 then
			redis.call('ZREM', queuePrioritiesKey, tostring(queuePriority))
		end
	end
end

do
	local _qd = tonumber(redis.call('ZINCRBY', 'srq:queues', -1, queueName))
	if _qd < 0 then
		redis.call('ZADD', 'srq:queues', 0, queueName)
	end
end

return {'removed', payload}
`)

// removeByTagScript удаляет до limit ожидающих задач с указанным тегом.
// ARGV[1] = queue name
// ARGV[2] = tag
// ARGV[3] = limit (1..1000)
// ARGV[4] = returnPayload ("1" / "0")
// Возврат: плоский массив {taskId, status, payload, ...}
// Приоритет в ответе: removed; missing/inprogress добивают до limit и вытесняются removed.
var removeByTagScript = redis.NewScript(`
local queueName = ARGV[1]
local tag = ARGV[2]
local limit = tonumber(ARGV[3]) or 100
local returnPayload = ARGV[4] == "1"

if limit < 1 then
	limit = 1
end
if limit > 1000 then
	limit = 1000
end

local tagKey = "queue:" .. queueName .. ":tag:" .. tag

local function removeFromTag(key, taskId)
	redis.call('SREM', key, taskId)
	if redis.call('SCARD', key) == 0 then
		redis.call('DEL', key)
	end
end

local function cleanupTags(taskId)
	local tagsKey = "queue:" .. queueName .. ":tags:" .. taskId
	local tags = redis.call('SMEMBERS', tagsKey)
	for t = 1, #tags do
		removeFromTag("queue:" .. queueName .. ":tag:" .. tags[t], taskId)
	end
	redis.call('DEL', tagsKey)
	removeFromTag(tagKey, taskId)
end

local function qpPrefix(qp)
	qp = tonumber(qp) or 0
	if qp == 0 then return "" end
	return "qp" .. tostring(qp) .. ":"
end

local queuePrioritiesKey = "queue:" .. queueName .. ":priorities"
local function groupReadyAnywhere(g)
	if redis.call('ZCARD', "queue:" .. queueName .. ":group:" .. g .. ":ready") > 0 then
		return true
	end
	local qpris = redis.call('ZRANGE', queuePrioritiesKey, 0, -1)
	for qi = 1, #qpris do
		local qp = tonumber(qpris[qi]) or 0
		if qp > 0 then
			if redis.call('ZCARD', "queue:" .. queueName .. ":" .. qpPrefix(qp) .. "group:" .. g .. ":ready") > 0 then
				return true
			end
		end
	end
	return false
end

local function partitionInAnyLane(part)
	if redis.call('SISMEMBER', "queue:" .. queueName .. ":partitions", part) == 1 then
		return true
	end
	local qpris = redis.call('ZRANGE', queuePrioritiesKey, 0, -1)
	for qi = 1, #qpris do
		local qp = tonumber(qpris[qi]) or 0
		if qp > 0 then
			if redis.call('SISMEMBER', "queue:" .. queueName .. ":" .. qpPrefix(qp) .. "partitions", part) == 1 then
				return true
			end
		end
	end
	return false
end

local function removeOne(taskId)
	local payloadKey = "queue:" .. queueName .. ":payload:" .. taskId
	local partitionKey = "queue:" .. queueName .. ":partition:" .. taskId
	local priorityKey = "queue:" .. queueName .. ":priority:" .. taskId
	local qpriorityKey = "queue:" .. queueName .. ":qpriority:" .. taskId

	local meta = redis.call('MGET', partitionKey, priorityKey, qpriorityKey)
	local partition = meta[1]
	if not partition then
		cleanupTags(taskId)
		return 'missing', ''
	end

	local priority = meta[2] or '0'
	if priority == false then
		priority = '0'
	end
	local queuePriority = 0
	if meta[3] ~= false then
		queuePriority = tonumber(meta[3]) or 0
	end
	local pref = qpPrefix(queuePriority)
	local queueKey = "queue:" .. queueName .. ":" .. pref .. "partition:" .. partition .. ":" .. priority
	local dlqKey = "queue:" .. queueName .. ":dlq"

	if redis.call('ZSCORE', dlqKey, taskId) then
		local payload = redis.call('GET', payloadKey) or ''
		redis.call('ZREM', dlqKey, taskId)
		redis.call('DEL', payloadKey, partitionKey, priorityKey, qpriorityKey,
			"queue:" .. queueName .. ":dlq:score:" .. taskId,
			"queue:" .. queueName .. ":reject_count:" .. taskId,
			"queue:" .. queueName .. ":step:" .. taskId,
			"queue:" .. queueName .. ":headers:" .. taskId,
			"queue:" .. queueName .. ":race:tags:" .. taskId,
			"queue:" .. queueName .. ":debounce:" .. taskId)
		cleanupTags(taskId)
		return 'removed', payload
	end

	if redis.call('ZREM', queueKey, taskId) == 0 then
		return 'inprogress', ''
	end

	local payload = redis.call('GET', payloadKey) or ''
	redis.call('DEL', payloadKey, partitionKey, priorityKey, qpriorityKey,
		"queue:" .. queueName .. ":reject_count:" .. taskId,
		"queue:" .. queueName .. ":step:" .. taskId,
		"queue:" .. queueName .. ":headers:" .. taskId)
	cleanupTags(taskId)

	local prioritiesKey = "queue:" .. queueName .. ":" .. pref .. "partition:" .. partition .. ":priorities"
	if redis.call('ZCARD', queueKey) == 0 then
		redis.call('ZREM', prioritiesKey, priority)
		if redis.call('ZCARD', prioritiesKey) == 0 then
			redis.call('SREM', "queue:" .. queueName .. ":" .. pref .. "partitions", partition)
			local t = redis.call('TIME')
			local nowMs = t[1] * 1000 + math.floor(t[2] / 1000)
			local groupsKey = "queue:" .. queueName .. ":groups"
			local pgroups = redis.call('SMEMBERS', "queue:" .. queueName .. ":partition:" .. partition .. ":groups")
			for gi = 1, #pgroups do
				local g = pgroups[gi]
				local readyKey = "queue:" .. queueName .. ":" .. pref .. "group:" .. g .. ":ready"
				redis.call('ZREM', readyKey, partition)
				if redis.call('ZCARD', readyKey) == 0 then
					redis.call('DEL', readyKey)
					if not groupReadyAnywhere(g) then
						local gs = redis.call('ZSCORE', groupsKey, g)
						if (not gs) or (tonumber(gs) <= nowMs) then
							redis.call('ZREM', groupsKey, g)
						end
					end
				end
			end
			if not partitionInAnyLane(partition) then
				redis.call('DEL', "queue:" .. queueName .. ":partition:" .. partition .. ":groups")
			end
			if pref ~= "" and redis.call('SCARD', "queue:" .. queueName .. ":" .. pref .. "partitions") == 0 then
				redis.call('ZREM', queuePrioritiesKey, tostring(queuePriority))
			end
		end
	end

	return 'removed', payload
end

local removed = {}
local other = {}
local seen = {}
local cursor = "0"

repeat
	local scan = redis.call('SSCAN', tagKey, cursor, 'COUNT', tostring(math.min(limit * 2, 200)))
	cursor = scan[1]
	local members = scan[2]

	for i = 1, #members do
		local taskId = members[i]
		if not seen[taskId] then
			seen[taskId] = true
			local status, payload = removeOne(taskId)
			local entry = {taskId, status, payload}
			if status == 'removed' then
				removed[#removed + 1] = entry
			elseif #other < limit then
				-- Храним не больше limit non-removed: при миллионах inprogress не раздуваем Lua.
				other[#other + 1] = entry
			end
			if #removed >= limit then
				break
			end
		end
	end
until cursor == "0" or #removed >= limit

local out = {}
for i = 1, #removed do
	if #out / 3 >= limit then
		break
	end
	out[#out + 1] = removed[i][1]
	out[#out + 1] = removed[i][2]
	if returnPayload then
		out[#out + 1] = removed[i][3]
	else
		out[#out + 1] = ''
	end
end
for i = 1, #other do
	if #out / 3 >= limit then
		break
	end
	out[#out + 1] = other[i][1]
	out[#out + 1] = other[i][2]
	if returnPayload then
		out[#out + 1] = other[i][3]
	else
		out[#out + 1] = ''
	end
end

if #removed > 0 then
	local _qd = tonumber(redis.call('ZINCRBY', 'srq:queues', -#removed, queueName))
	if _qd < 0 then
		redis.call('ZADD', 'srq:queues', 0, queueName)
	end
end

return out
`)

// Функции для получения скриптов

func getAddScript() *redis.Script {
	return addScript
}

func getLockScript() *redis.Script {
	return lockScript
}

func getGetScript() *redis.Script {
	return getScript
}

func getAckScript() *redis.Script {
	return ackScript
}

// shareScript — mid-handler: shared:{id} blobs и/или block:{id}=ready.
// ARGV[1]=queue ARGV[2]=taskId ARGV[3]=consumerId ARGV[4]=sharedTTL
// ARGV[5]=concurrencyTags ARGV[6]=nShared ARGV[7..]=pairs; затем nOpen, ids...
var shareScript = redis.NewScript(`
local queueName = ARGV[1]
local taskId = ARGV[2]
local consumerId = ARGV[3]
local sharedTtl = tonumber(ARGV[4]) or 86400
local concurrencyTags = (ARGV[5] or "") == "1"
local nShared = tonumber(ARGV[6]) or 0
if sharedTtl < 1 then
	sharedTtl = 86400
end

local consumerTasksKey = "queue:" .. queueName .. ":consumer:" .. consumerId .. ":tasks"
if redis.call('HGET', consumerTasksKey, taskId) == false then
	return 0
end

local raceLocksKey = "queue:" .. queueName .. ":race:locks"
local raceSharedPrefix = "race!s!"
local raceBlockPrefix = "race!b!"
local argi = 7
local toDel = {}
for _ = 1, nShared do
	local id = ARGV[argi]
	local blob = ARGV[argi + 1]
	argi = argi + 2
	if id ~= nil and id ~= "" and blob ~= nil then
		redis.call('SET', "queue:" .. queueName .. ":shared:" .. id, blob, 'EX', math.floor(sharedTtl))
		if concurrencyTags then
			local tag = raceSharedPrefix .. id
			if redis.call('HGET', raceLocksKey, tag) == taskId then
				toDel[#toDel + 1] = tag
			end
		end
	end
end
local nOpen = tonumber(ARGV[argi]) or 0
argi = argi + 1
for _ = 1, nOpen do
	local id = ARGV[argi]
	argi = argi + 1
	if id ~= nil and id ~= "" then
		redis.call('SET', "queue:" .. queueName .. ":block:" .. id, "ready", 'EX', math.floor(sharedTtl))
		if concurrencyTags then
			local tag = raceBlockPrefix .. id
			if redis.call('HGET', raceLocksKey, tag) == taskId then
				toDel[#toDel + 1] = tag
			end
		end
	end
end
if #toDel > 0 then
	redis.call('HDEL', raceLocksKey, unpack(toDel))
end
return 1
`)

func getShareScript() *redis.Script {
	return shareScript
}

func getRejectScript() *redis.Script {
	return rejectScript
}

func getBlockGroupsScript() *redis.Script {
	return blockGroupsScript
}

func getPingScript() *redis.Script {
	return pingScript
}

func getRemoveScript() *redis.Script {
	return removeScript
}

func getRemoveByTagScript() *redis.Script {
	return removeByTagScript
}

// sampleTagsStatsScript: раз в секунду один воркер пишет SCARD выбранных тегов в stats:{sec}.
// ARGV: queueName, retentionSec, tag1, tag2, ...
var sampleTagsStatsScript = redis.NewScript(`
local queueName = ARGV[1]
local retention = tonumber(ARGV[2]) or 3600
if retention < 1 then
	retention = 3600
end
local t = redis.call('TIME')
local sec = tonumber(t[1])
local key = "queue:" .. queueName .. ":stats:" .. sec
local indexKey = "queue:" .. queueName .. ":stats:index"
local wrote = 0
for i = 3, #ARGV do
	local tag = ARGV[i]
	if tag ~= nil and tag ~= "" then
		local field = "t:" .. tag
		if redis.call('HEXISTS', key, field) == 0 then
			local n = redis.call('SCARD', "queue:" .. queueName .. ":tag:" .. tag)
			redis.call('HSET', key, field, n)
			wrote = wrote + 1
		end
	end
end
if wrote > 0 or redis.call('EXISTS', key) == 1 then
	redis.call('EXPIRE', key, retention)
	redis.call('ZADD', indexKey, sec, tostring(sec))
	redis.call('ZREMRANGEBYSCORE', indexKey, '-inf', tostring(sec - retention))
end
return wrote
`)

func getSampleTagsStatsScript() *redis.Script {
	return sampleTagsStatsScript
}
