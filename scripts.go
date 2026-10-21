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

// getAddScript возвращает Lua скрипт для добавления задач (батч)
// ARGV[1] = queue name
// ARGV[2] = количество задач
// ARGV[3] = bpCount (число лимитов backpressure; 0 = выкл)
// ARGV[4 .. 3+2*bpCount] = пары tag, limit
// Далее группы по 7 аргументов на задачу (сдвиг taskBase = 4 + 2*bpCount):
//
//	ARGV[taskBase + i*7] = task ID
//	… partition, priority, scheduled, payload, tags, groups
//
// Возвращает:
//
//	— список индексов не добавленных (дубликаты NX);
//	— {'!', 'backpressure', tag, count, limit} если SCARD+taskCount > limit (ничего не добавлено).
var addScript = redis.NewScript(`
local queueName = ARGV[1]
local taskCount = tonumber(ARGV[2])
local bpCount = tonumber(ARGV[3]) or 0

-- F3: backpressure по тегам — до любых SET; fail → весь батч не пишется
if bpCount > 0 then
	for bi = 0, bpCount - 1 do
		local tag = ARGV[4 + bi * 2]
		local limit = tonumber(ARGV[5 + bi * 2]) or 0
		if tag ~= "" and limit > 0 then
			local n = redis.call('SCARD', "queue:" .. queueName .. ":tag:" .. tag)
			if n + taskCount > limit then
				return {'!', 'backpressure', tag, tostring(n), tostring(limit)}
			end
		end
	end
end

local taskBase = 4 + bpCount * 2
local partitionsKey = "queue:" .. queueName .. ":partitions"
-- groups: ZSET score=0|past (свободна) | unlockAt>now (блок). Единственный источник правды для групп.
local groupsKey = "queue:" .. queueName .. ":groups"

local function ensureGroup(group)
	if not redis.call('ZSCORE', groupsKey, group) then
		redis.call('ZADD', groupsKey, 0, group)
	end
end

-- Поднять partition в ready всех её групп (после ZADD задачи).
local function ensurePartitionReady(partitionCode)
	local partGroupsKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":groups"
	local pgroups = redis.call('SMEMBERS', partGroupsKey)
	if #pgroups == 0 then
		pgroups = {"_default"}
		redis.call('SADD', partGroupsKey, "_default")
	end
	for gi = 1, #pgroups do
		local g = pgroups[gi]
		ensureGroup(g)
		redis.call('ZADD', "queue:" .. queueName .. ":group:" .. g .. ":ready", 0, partitionCode)
	end
end

local debPrefix = "deb!"
local notAddedItems = {}

for i = 0, taskCount - 1 do
    local base = taskBase + i * 7
    local taskId = ARGV[base]
    local partitionCode = ARGV[base + 1]
	if partitionCode == "" then
        partitionCode = "base"
	end
    local priority = ARGV[base + 2]
    local scheduled = tonumber(ARGV[base + 3])
    local payload = ARGV[base + 4]
    local tagsStr = ARGV[base + 5] or ""
    local groupsStr = ARGV[base + 6] or ""

    local payloadKey = "queue:" .. queueName .. ":payload:" .. taskId
    local partitionKey = "queue:" .. queueName .. ":partition:" .. taskId
    local priorityKey = "queue:" .. queueName .. ":priority:" .. taskId
	local isDebounce = taskId:sub(1, #debPrefix) == debPrefix

    local ok = redis.call('SET', payloadKey, payload, 'NX')
    if ok then
		redis.call('SET', partitionKey, partitionCode)
		redis.call('SET', priorityKey, priority)

		local partitionQueueKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":" .. priority
		redis.call('ZADD', partitionQueueKey, scheduled, taskId)
		redis.call('SADD', partitionsKey, partitionCode)

		local prioritiesKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":priorities"
		redis.call('ZADD', prioritiesKey, priority, priority)

		if tagsStr ~= "" then
			local tagsKey = "queue:" .. queueName .. ":tags:" .. taskId
			local raceTagsKey = "queue:" .. queueName .. ":race:tags:" .. taskId
			local racePrefix = "race!"
			for tag in string.gmatch(tagsStr, "[^#]+") do
				if tag ~= "" then
					redis.call('SADD', "queue:" .. queueName .. ":tag:" .. tag, taskId)
					redis.call('SADD', tagsKey, tag)
					-- F5: race! → отдельный SET для Get (без опций на Publish)
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
					redis.call('ZADD', "queue:" .. queueName .. ":group:" .. group .. ":ready", 0, partitionCode)
					redis.call('SADD', partGroupsKey, group)
				end
			end
		end
		if not hasGroup then
			ensureGroup("_default")
			redis.call('ZADD', "queue:" .. queueName .. ":group:_default:ready", 0, partitionCode)
			redis.call('SADD', partGroupsKey, "_default")
		end
    elseif isDebounce then
		-- F2: payload уже есть — waiting (в ZSET) или in-flight (нет в ZSET)
		local debounceKey = "queue:" .. queueName .. ":debounce:" .. taskId
		local existingPart = redis.call('GET', partitionKey)
		if existingPart == false then
			existingPart = "base"
		end
		local existingPrio = redis.call('GET', priorityKey)
		if existingPrio == false then
			existingPrio = "0"
		end
		local oldQueueKey = "queue:" .. queueName .. ":partition:" .. existingPart .. ":" .. existingPrio
		local inQueue = redis.call('ZSCORE', oldQueueKey, taskId)

		redis.call('SET', payloadKey, payload)

		if inQueue ~= false then
			-- waiting: сдвинуть score; при смене part/prio — перенести member
			if existingPart ~= partitionCode or tostring(existingPrio) ~= tostring(priority) then
				redis.call('ZREM', oldQueueKey, taskId)
				redis.call('SET', partitionKey, partitionCode)
				redis.call('SET', priorityKey, priority)
			end
			local partitionQueueKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":" .. priority
			redis.call('ZADD', partitionQueueKey, scheduled, taskId)
			redis.call('SADD', partitionsKey, partitionCode)
			local prioritiesKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":priorities"
			redis.call('ZADD', prioritiesKey, priority, priority)
			ensurePartitionReady(partitionCode)
			redis.call('DEL', debounceKey)
		else
			-- in-flight: только payload + флаг until (без ZADD)
			redis.call('SET', debounceKey, tostring(scheduled))
		end
    else
		notAddedItems[#notAddedItems + 1] = i
    end
end

return notAddedItems
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
// Возвращает: {taskId1, partition1, payload1, rejectCount1, ...} или {}
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
local raceLocksKey = "queue:" .. queueName .. ":race:locks"
local raceLocks = nil
local raceSkips = 0
local maxRaceSkips = 64
local raceBudgetExhausted = false

-- Вычисляем текущее время
local currentTime = redis.call('TIME')
local currentUnix = currentTime[1]
local now = currentUnix * 1000 + math.floor(currentTime[2] / 1000)
local nowSec = now / 1000.0

-- Формируем ключи
local partitionsKey = "queue:" .. queueName .. ":partitions"
local consumersKey = "queue:" .. queueName .. ":consumers"
local consumerKey = "queue:" .. queueName .. ":consumer:" .. consumerId
local consumerTasksKey = "queue:" .. queueName .. ":consumer:" .. consumerId .. ":tasks"

local acquiredPartitionLocks = {}
local partitionHadTask = {}
-- R4/D4: партиции с головой score>now — не заходим снова до конца этого Get
local deferredPartitions = {}

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

-- true = можно брать; false = race busy (partition deferred).
-- EVAL атомарный: между проверкой map и HSET другой клиент не вклинится.
-- HSET (не HSETNX): снимок raceLocks уже полный для этого скрипта.
local function tryAcquireRace(taskId, partition)
	if not concurrencyTags then
		return true
	end
	local raceTagsKey = "queue:" .. queueName .. ":race:tags:" .. taskId
	if redis.call('EXISTS', raceTagsKey) == 0 then
		return true
	end
	local tags = redis.call('SMEMBERS', raceTagsKey)
	if #tags == 0 then
		return true
	end
	ensureRaceLocks()
	for i = 1, #tags do
		if raceLocks[tags[i]] then
			deferredPartitions[partition] = true
			raceSkips = raceSkips + 1
			if raceSkips >= maxRaceSkips then
				raceBudgetExhausted = true
			end
			return false
		end
	end
	for i = 1, #tags do
		local t = tags[i]
		redis.call('HSET', raceLocksKey, t, taskId)
		raceLocks[t] = taskId
	end
	return true
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

local function removePartitionFromIndexes(partition)
	redis.call('SREM', partitionsKey, partition)
	local pgroups = redis.call('SMEMBERS', "queue:" .. queueName .. ":partition:" .. partition .. ":groups")
	for gi = 1, #pgroups do
		local g = pgroups[gi]
		local readyKey = "queue:" .. queueName .. ":group:" .. g .. ":ready"
		redis.call('ZREM', readyKey, partition)
		if redis.call('ZCARD', readyKey) == 0 then
			redis.call('DEL', readyKey)
			-- не снимаем rate-limit: ZREM groups только если score <= now
			local gs = redis.call('ZSCORE', groupsKey, g)
			if (not gs) or (tonumber(gs) <= now) then
				redis.call('ZREM', groupsKey, g)
			end
		end
	end
	redis.call('DEL', "queue:" .. queueName .. ":partition:" .. partition .. ":groups")
	redis.call('DEL', "queue:" .. queueName .. ":partition:" .. partition .. ":priorities")
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
	local prioritiesKey = "queue:" .. queueName .. ":partition:" .. partition .. ":priorities"
	local priorities = redis.call('ZREVRANGE', prioritiesKey, 0, -1)
	for p = 1, #priorities do
		local maxPriority = priorities[p]
		local partitionQueueKey = "queue:" .. queueName .. ":partition:" .. partition .. ":" .. maxPriority
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
				-- F5: race! locks (opt-in) до ZREM
				if not tryAcquireRace(taskId, partition) then
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
				local payload = redis.call('GET', payloadKey)
				local rejectCountKey = "queue:" .. queueName .. ":reject_count:" .. taskId
				local rejectCount = tonumber(redis.call('GET', rejectCountKey)) or 0
				-- partition уже известен (мы внутри getFromPartition); GET partition:{id} не нужен
				return {taskId, partition, payload or "", tostring(rejectCount)}
			end
		else
			redis.call('ZREM', prioritiesKey, maxPriority)
		end
	end

	-- дошли сюда без return: либо пусто, либо только future head(s)
	if found then
		deferredPartitions[partition] = true
	else
		removePartitionFromIndexes(partition)
	end
    
    return nil
end

-- group — текущая группа обхода (из tryReadyGroup); RL group-слой считает/блокирует именно её
local function tryCollectFromPartition(partition, group)
	if deferredPartitions[partition] then
		return nil
	end
	if isAdmissionBlocked(partition) then
		return nil
	end
	return getFromPartition(partition, group)
end

-- ready = ZSET (score=0). Random start index → ZRANGE батчами по кругу. Память O(batch).
local function tryReadyGroup(g)
	local readyKey = "queue:" .. queueName .. ":group:" .. g .. ":ready"
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
				return taskData
			end
		end
		seen = seen + take
	end
	return nil
end

-- Обход свободных групп: ZCOUNT + ZRANGEBYSCORE LIMIT батчами (память O(batch)),
-- random start по кругу — как tryReadyGroup. Если mid-walk F4 сделал ZADD
-- future score, free-set сжался и offset-walk ломается → refresh ZCOUNT и заново.
local function collectOne()
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
					-- F4 заблокировал эту группу → n/offset устарели
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

-- Основной цикл: набираем до prefetchCount задач (4 элемента на задачу: id, partition, payload, rejectCount)
while #results / 4 < prefetchCount do
	if raceBudgetExhausted then
		break
	end
	local taskData = collectOne()
	if not taskData then
		break
	end
	results[#results + 1] = taskData[1]
	results[#results + 1] = taskData[2]
	results[#results + 1] = taskData[3]
	results[#results + 1] = taskData[4]
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
var ackScript = redis.NewScript(`
local queueName = ARGV[1]
local taskId = ARGV[2]
local consumerId = ARGV[3]
local idempotencyTtl = tonumber(ARGV[4])
local concurrencyTags = (ARGV[5] or "") == "1"

-- Формируем ключи
local payloadKey = "queue:" .. queueName .. ":payload:" .. taskId
local partitionKey = "queue:" .. queueName .. ":partition:" .. taskId
local priorityKey = "queue:" .. queueName .. ":priority:" .. taskId
local debPrefix = "deb!"
local isDebounce = taskId:sub(1, #debPrefix) == debPrefix
local raceLocksKey = "queue:" .. queueName .. ":race:locks"
local raceTagsKey = "queue:" .. queueName .. ":race:tags:" .. taskId

-- F5: снять локи только если consumer включил concurrency (acquire был только тогда)
local function releaseRaceLocks()
	if not concurrencyTags then
		return
	end
	if redis.call('EXISTS', raceTagsKey) == 0 then
		return
	end
	local tags = redis.call('SMEMBERS', raceTagsKey)
	for i = 1, #tags do
		if redis.call('HGET', raceLocksKey, tags[i]) == taskId then
			redis.call('HDEL', raceLocksKey, tags[i])
		end
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
		redis.call('DEL', consumerPartitionCountKey)
		local partitionLockKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":lock"
		local lockOwner = redis.call('GET', partitionLockKey)
		if lockOwner == consumerId then
			redis.call('DEL', partitionLockKey)
		end
	end
end

redis.call('HDEL', consumerTasksKey, taskId)
releaseRaceLocks()

-- F2: только deb! id — GET флага/priority и reactivate; иначе нулевой оверхед
if isDebounce then
	local debounceKey = "queue:" .. queueName .. ":debounce:" .. taskId
	local debounceUntil = redis.call('GET', debounceKey)
	if debounceUntil ~= false then
		local priorityVal = redis.call('GET', priorityKey)
		local priority = 0
		if priorityVal then
			priority = tonumber(priorityVal) or 0
		end
		local score = tonumber(debounceUntil) or 0
		local partitionQueueKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":" .. tostring(priority)
		redis.call('ZADD', partitionQueueKey, score, taskId)
		redis.call('SADD', "queue:" .. queueName .. ":partitions", partitionCode)
		redis.call('ZADD', "queue:" .. queueName .. ":partition:" .. partitionCode .. ":priorities", priority, tostring(priority))
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
			redis.call('ZADD', "queue:" .. queueName .. ":group:" .. g .. ":ready", 0, partitionCode)
		end
		redis.call('DEL', debounceKey)
		-- race:tags оставляем — задача снова в очереди
		return 1
	end
end

-- Удаляем payload, ключ партиции и reject_count
if idempotencyTtl > 0 then
	redis.call('SET', payloadKey, '0')
	redis.call('EXPIRE', payloadKey, idempotencyTtl)
else
	redis.call('DEL', payloadKey)
end
redis.call('DEL', partitionKey, priorityKey,
	"queue:" .. queueName .. ":reject_count:" .. taskId,
	"queue:" .. queueName .. ":step:" .. taskId,
	"queue:" .. queueName .. ":headers:" .. taskId,
	raceTagsKey)

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

return 1
`)

// getRejectScript возвращает Lua скрипт для отклонения задачи.
// Возврат в тот же priority с originalScore из consumer:tasks.
// Так prefetch/reject и dead-reclaim не ломают порядок без priority+1 / reject_seq.
// При waitTime > 0: ordered → blocked {partition}; группы → score unlockAt в groups.
// ARGV[1] = queue name
// ARGV[2] = task ID
// ARGV[3] = consumer ID
// ARGV[4] = waitTime в секундах (дробное OK; 0 = без блокировки)
// ARGV[5] = blockGroups (g1#g2#..., опционально)
// ARGV[6] = concurrencyTags ("1" / "0") — F5 release race locks
var rejectScript = redis.NewScript(`
local queueName = ARGV[1]
local taskId = ARGV[2]
local consumerId = ARGV[3]
local waitTime = tonumber(ARGV[4]) or 0
local blockGroupsStr = ARGV[5] or ""
local concurrencyTags = (ARGV[6] or "") == "1"

local consumerTasksKey = "queue:" .. queueName .. ":consumer:" .. consumerId .. ":tasks"

-- Проверяем что задача в processing и принадлежит этому консьюмеру
local meta = redis.call('HGET', consumerTasksKey, taskId)
if meta == false then
    return 0
end

-- Получаем партицию
local partitionKey = "queue:" .. queueName .. ":partition:" .. taskId
local partitionCode = redis.call('GET', partitionKey) or "base"
local priorityKey = "queue:" .. queueName .. ":priority:" .. taskId
local priorityVal = redis.call('GET', priorityKey)
local priority = 0
if priorityVal then
    priority = tonumber(priorityVal) or 0
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

-- Возвращаем задачу обратно в очередь (тот же priority + исходный score)
local partitionQueueKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":" .. priority
redis.call('ZADD', partitionQueueKey, score, taskId)
local partitionsKey = "queue:" .. queueName .. ":partitions"
local groupsKey = "queue:" .. queueName .. ":groups"
redis.call('SADD', partitionsKey, partitionCode)
local prioritiesKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":priorities"
redis.call('ZADD', prioritiesKey, priority, tostring(priority))

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
	redis.call('ZADD', "queue:" .. queueName .. ":group:" .. g .. ":ready", 0, partitionCode)
end

-- :count / :lock только для ordered (!)
if needsLockUnlock then
	local consumerPartitionCountKey = "queue:" .. queueName .. ":consumer:" .. consumerId .. ":partition:" .. partitionCode .. ":count"
	local consumerPartitionCount = redis.call('DECR', consumerPartitionCountKey) or 0
	if consumerPartitionCount <= 0 then
		redis.call('DEL', consumerPartitionCountKey)
		local partitionLockKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":lock"
		local lockOwner = redis.call('GET', partitionLockKey)
		if lockOwner == consumerId then
			redis.call('DEL', partitionLockKey)
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
				redis.call('ZADD', groupsKey, unlockAt, group)
			end
		end
	end
end

-- Удаляем задачу из hash консьюмера (prefetch)
redis.call('HDEL', consumerTasksKey, taskId)
-- F2: снова в ZSET → снять флаг только для deb! id
local debPrefix = "deb!"
if taskId:sub(1, #debPrefix) == debPrefix then
	redis.call('DEL', "queue:" .. queueName .. ":debounce:" .. taskId)
end
-- F5: снять race locks (задача снова waiting; race:tags оставляем)
if concurrencyTags then
	local raceLocksKey = "queue:" .. queueName .. ":race:locks"
	local raceTagsKey = "queue:" .. queueName .. ":race:tags:" .. taskId
	local rtags = redis.call('SMEMBERS', raceTagsKey)
	for i = 1, #rtags do
		if redis.call('HGET', raceLocksKey, rtags[i]) == taskId then
			redis.call('HDEL', raceLocksKey, rtags[i])
		end
	end
end

return 1
`)

// blockGroupsScript паркует группы без привязки к задаче (Consumer/Admin.BlockGroups).
// ARGV[1] = queue name
// ARGV[2] = waitTime секунды
// ARGV[3] = groups (g1#g2#...)
var blockGroupsScript = redis.NewScript(`
local queueName = ARGV[1]
local waitTime = tonumber(ARGV[2]) or 0
local groupsStr = ARGV[3] or ""
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
		redis.call('ZADD', groupsKey, unlockAt, group)
		n = n + 1
	end
end
return n
`)

// getPingScript возвращает Lua скрипт для ping консьюмера и разблокировки мертвых консьюмеров
// ARGV[1] = queue name
// ARGV[2] = consumer ID
var pingScript = redis.NewScript(`
local queueName = ARGV[1]
local consumerId = ARGV[2]

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
		local partitionsKey = "queue:" .. queueName .. ":partitions"
		local restoredParts = {}
		local partPriorities = {}

		for j = 1, #tasksData, 2 do
			local taskToReturn = tasksData[j]
			local meta = tasksData[j + 1]
			if taskToReturn then
				local taskPartitionKey = "queue:" .. queueName .. ":partition:" .. taskToReturn
				local taskPartition = redis.call('GET', taskPartitionKey)
				if taskPartition == false then
					taskPartition = "base"
				end

				local taskPriorityKey = "queue:" .. queueName .. ":priority:" .. taskToReturn
				local taskPriorityVal = redis.call('GET', taskPriorityKey)
				local taskPriority = 0
				if taskPriorityVal then
					taskPriority = tonumber(taskPriorityVal) or 0
				end

				local score = tonumber(meta) or now
				local partitionQueueKey = "queue:" .. queueName .. ":partition:" .. taskPartition .. ":" .. taskPriority
				redis.call('ZADD', partitionQueueKey, score, taskToReturn)
				-- F2: reclaim → DEL debounce только для deb! id
				local debPrefix = "deb!"
				if taskToReturn:sub(1, #debPrefix) == debPrefix then
					redis.call('DEL', "queue:" .. queueName .. ":debounce:" .. taskToReturn)
				end
				-- F5: reclaim всегда снимает race locks этого taskId (иначе утечка)
				local raceLocksKey = "queue:" .. queueName .. ":race:locks"
				local raceTagsKey = "queue:" .. queueName .. ":race:tags:" .. taskToReturn
				if redis.call('EXISTS', raceTagsKey) == 1 then
					local rtags = redis.call('SMEMBERS', raceTagsKey)
					for ri = 1, #rtags do
						if redis.call('HGET', raceLocksKey, rtags[ri]) == taskToReturn then
							redis.call('HDEL', raceLocksKey, rtags[ri])
						end
					end
				end

				restoredParts[taskPartition] = true
				if not partPriorities[taskPartition] then
					partPriorities[taskPartition] = {}
				end
				partPriorities[taskPartition][tostring(taskPriority)] = taskPriority
			end
		end

		for part, _ in pairs(restoredParts) do
			redis.call('SADD', partitionsKey, part)
			local prioritiesKey = "queue:" .. queueName .. ":partition:" .. part .. ":priorities"
			for prioStr, prioNum in pairs(partPriorities[part]) do
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
				redis.call('ZADD', "queue:" .. queueName .. ":group:" .. g .. ":ready", 0, part)
			end

			if part:sub(1, 1) == "!" then
				redis.call('DEL', "queue:" .. queueName .. ":consumer:" .. lockOwner .. ":partition:" .. part .. ":count")
				redis.call('DEL', "queue:" .. queueName .. ":partition:" .. part .. ":lock")
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
local tagsKey = "queue:" .. queueName .. ":tags:" .. taskId

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

local partition = redis.call('GET', partitionKey)
if not partition then
	-- Возможны осиротевшие индексы тегов (старый ack без чистки тегов) — подчистим.
	cleanupTags()
	return {'missing'}
end

local priority = redis.call('GET', priorityKey) or '0'
local queueKey = "queue:" .. queueName .. ":partition:" .. partition .. ":" .. priority

if redis.call('ZREM', queueKey, taskId) == 0 then
	-- Задача взята консьюмером (или уже не в ZSET) — не трогаем.
	return {'inprogress'}
end

local payload = redis.call('GET', payloadKey) or ''
redis.call('DEL', payloadKey, partitionKey, priorityKey,
	"queue:" .. queueName .. ":reject_count:" .. taskId,
	"queue:" .. queueName .. ":step:" .. taskId,
	"queue:" .. queueName .. ":headers:" .. taskId)
cleanupTags()
-- F5: waiting remove — race:tags + возможные locks (value=taskId)
local raceTagsKey = "queue:" .. queueName .. ":race:tags:" .. taskId
local raceLocksKey = "queue:" .. queueName .. ":race:locks"
local rtags = redis.call('SMEMBERS', raceTagsKey)
for i = 1, #rtags do
	if redis.call('HGET', raceLocksKey, rtags[i]) == taskId then
		redis.call('HDEL', raceLocksKey, rtags[i])
	end
end
redis.call('DEL', raceTagsKey)

local prioritiesKey = "queue:" .. queueName .. ":partition:" .. partition .. ":priorities"
if redis.call('ZCARD', queueKey) == 0 then
	redis.call('ZREM', prioritiesKey, priority)
	if redis.call('ZCARD', prioritiesKey) == 0 then
		redis.call('SREM', "queue:" .. queueName .. ":partitions", partition)
		local t = redis.call('TIME')
		local nowMs = t[1] * 1000 + math.floor(t[2] / 1000)
		local groupsKey = "queue:" .. queueName .. ":groups"
		local pgroups = redis.call('SMEMBERS', "queue:" .. queueName .. ":partition:" .. partition .. ":groups")
		for gi = 1, #pgroups do
			local g = pgroups[gi]
			local readyKey = "queue:" .. queueName .. ":group:" .. g .. ":ready"
			redis.call('ZREM', readyKey, partition)
			if redis.call('ZCARD', readyKey) == 0 then
				redis.call('DEL', readyKey)
				local gs = redis.call('ZSCORE', groupsKey, g)
				if (not gs) or (tonumber(gs) <= nowMs) then
					redis.call('ZREM', groupsKey, g)
				end
			end
		end
		redis.call('DEL', "queue:" .. queueName .. ":partition:" .. partition .. ":groups")
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
	-- На случай отсутствия reverse-index всё равно убираем из текущего тега.
	removeFromTag(tagKey, taskId)
end

local function removeOne(taskId)
	local payloadKey = "queue:" .. queueName .. ":payload:" .. taskId
	local partitionKey = "queue:" .. queueName .. ":partition:" .. taskId
	local priorityKey = "queue:" .. queueName .. ":priority:" .. taskId

	local partition = redis.call('GET', partitionKey)
	if not partition then
		cleanupTags(taskId)
		return 'missing', ''
	end

	local priority = redis.call('GET', priorityKey) or '0'
	local queueKey = "queue:" .. queueName .. ":partition:" .. partition .. ":" .. priority

	if redis.call('ZREM', queueKey, taskId) == 0 then
		return 'inprogress', ''
	end

	local payload = redis.call('GET', payloadKey) or ''
	redis.call('DEL', payloadKey, partitionKey, priorityKey,
		"queue:" .. queueName .. ":reject_count:" .. taskId,
		"queue:" .. queueName .. ":step:" .. taskId,
		"queue:" .. queueName .. ":headers:" .. taskId)
	cleanupTags(taskId)

	local prioritiesKey = "queue:" .. queueName .. ":partition:" .. partition .. ":priorities"
	if redis.call('ZCARD', queueKey) == 0 then
		redis.call('ZREM', prioritiesKey, priority)
		if redis.call('ZCARD', prioritiesKey) == 0 then
			redis.call('SREM', "queue:" .. queueName .. ":partitions", partition)
			local t = redis.call('TIME')
			local nowMs = t[1] * 1000 + math.floor(t[2] / 1000)
			local groupsKey = "queue:" .. queueName .. ":groups"
			local pgroups = redis.call('SMEMBERS', "queue:" .. queueName .. ":partition:" .. partition .. ":groups")
			for gi = 1, #pgroups do
				local g = pgroups[gi]
				local readyKey = "queue:" .. queueName .. ":group:" .. g .. ":ready"
				redis.call('ZREM', readyKey, partition)
				if redis.call('ZCARD', readyKey) == 0 then
					redis.call('DEL', readyKey)
					local gs = redis.call('ZSCORE', groupsKey, g)
					if (not gs) or (tonumber(gs) <= nowMs) then
						redis.call('ZREM', groupsKey, g)
					end
				end
			end
			redis.call('DEL', "queue:" .. queueName .. ":partition:" .. partition .. ":groups")
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
