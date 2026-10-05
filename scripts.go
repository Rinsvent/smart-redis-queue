package redisqueue

import (
	"github.com/redis/go-redis/v9"
)

// TagSeparator — разделитель тегов в ARGV add-скрипта и в Redis-индексах.
// Тег не должен содержать этот символ. Выбран `#`: реже встречается в id/slug, чем `,` или `/`.
const TagSeparator = "#"

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
// Далее группы по 6 аргументов на задачу:
//
//	ARGV[3 + i*6] = task ID
//	ARGV[4 + i*6] = partition code (пустая строка = без партиции)
//	ARGV[5 + i*6] = priority
//	ARGV[6 + i*6] = scheduled timestamp (ms)
//	ARGV[7 + i*6] = payload
//	ARGV[8 + i*6] = tags (строка tag1#tag2#..., пустая = без тегов)
//
// Возвращает список не добавленных задач - их порядковый номер
var addScript = redis.NewScript(`
local queueName = ARGV[1]
local taskCount = tonumber(ARGV[2])

local partitionsKey = "queue:" .. queueName .. ":partitions"

local notAddedItems = {}

for i = 0, taskCount - 1 do
    local base = 3 + i * 6
    local taskId = ARGV[base]
    local partitionCode = ARGV[base + 1]
	if partitionCode == "" then
        partitionCode = "base"
	end
    local priority = ARGV[base + 2]
    local scheduled = tonumber(ARGV[base + 3])
    local payload = ARGV[base + 4]
    local tagsStr = ARGV[base + 5] or ""

    local payloadKey = "queue:" .. queueName .. ":payload:" .. taskId
    local partitionKey = "queue:" .. queueName .. ":partition:" .. taskId
    local priorityKey = "queue:" .. queueName .. ":priority:" .. taskId

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
			for tag in string.gmatch(tagsStr, "[^#]+") do
				if tag ~= "" then
					redis.call('SADD', "queue:" .. queueName .. ":tag:" .. tag, taskId)
					redis.call('SADD', tagsKey, tag)
				end
			end
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

    local ok = redis.call('SET', payloadKey, 0, 'NX')
    if ok == 0 then
		notAddedItems[#notAddedItems + 1] = i
    end
end

return notAddedItems
`)

// getGetScript возвращает Lua скрипт для получения до prefetchCount задач
// ARGV[1] = queue name
// ARGV[2] = consumer ID
// ARGV[3] = prefetch count
// Возвращает: {taskId1, partition1, payload1, taskId2, ...} или {}
var getScript = redis.NewScript(`
local queueName = ARGV[1]
local consumerId = ARGV[2]
local prefetchCount = tonumber(ARGV[3]) or 1
local checkDeadConsumerLocks = ARGV[4] == "1"
if prefetchCount < 1 then
    prefetchCount = 1
end

-- Вычисляем текущее время
local currentTime = redis.call('TIME')
local currentUnix = currentTime[1]
local now = currentUnix * 1000 + math.floor(currentTime[2] / 1000)

-- Формируем ключи
local queueKey = "queue:" .. queueName
local partitionsKey = "queue:" .. queueName .. ":partitions"
local consumersKey = "queue:" .. queueName .. ":consumers"
local consumerKey = "queue:" .. queueName .. ":consumer:" .. consumerId
local consumerTasksKey = "queue:" .. queueName .. ":consumer:" .. consumerId .. ":tasks"

local acquiredPartitionLocks = {}
local partitionHadTask = {}

-- Регистрируем/обновляем консьюмера
redis.call('SADD', consumersKey, consumerId)
redis.call('SET', consumerKey, currentUnix, 'EX', 120)

local results = {}

-- Функция для получения одной задачи из партиции
-- Партиции с префиксом "!" блокируются (эксклюзивны для одного консьюмера).
-- Тот же консьюмер может брать несколько задач подряд; блокировка снимается
-- только когда все задачи из партиции акнуты или реджекнуты.
-- Ключ :block блокирует партицию до unlockAt (ms) или пока жив legacy-ключ "1".
local function getFromPartition(partition)
    local needsLock = partition:sub(1, 1) == "!"
    
    if needsLock then
        -- Проверяем TTL-блок (ratelimit cooldown)
        local partitionBlockKey = "queue:" .. queueName .. ":partition:" .. partition .. ":block"
        local blockVal = redis.call('GET', partitionBlockKey)
        if blockVal ~= false then
            if blockVal == "1" then
                -- Старый формат: ключ с TTL, значение-маркер
                return nil
            end
            local unlockAt = tonumber(blockVal)
            if unlockAt and now < unlockAt then
                return nil
            end
            -- unlockAt уже прошёл, а TTL ключа ещё жив — партиция доступна
        end
        local partitionLockKey = "queue:" .. queueName .. ":partition:" .. partition .. ":lock"
        local lockOwner = redis.call('GET', partitionLockKey)
        if lockOwner == false then
            local lockAcquired = redis.call('SET', partitionLockKey, consumerId, 'NX')
            if not lockAcquired then
                return nil
            end
            acquiredPartitionLocks[partition] = true
        elseif lockOwner ~= consumerId then
            if checkDeadConsumerLocks then
                -- Снимаем stale-lock только когда lockOwner уже удалён из consumers (ping его “признал мёртвым”).
                -- Иначе можно нарушить порядок: задачи могли быть в обработке и ещё не возвращены в очередь.
                if redis.call('SISMEMBER', consumersKey, lockOwner) == 0 then
                    redis.call('DEL', partitionLockKey)
                end
            end
            return nil
        end
    end

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
				redis.call('ZREM', partitionQueueKey, taskId)
				redis.call('HSET', consumerTasksKey, taskId, now)
				redis.call('INCR', consumerPartitionCountKey)
				partitionHadTask[partition] = true
				local payloadKey = "queue:" .. queueName .. ":payload:" .. taskId
				local payload = redis.call('GET', payloadKey)
				local taskPartitionKey = "queue:" .. queueName .. ":partition:" .. taskId
				local taskPartition = redis.call('GET', taskPartitionKey) or "base"
				local rejectCountKey = "queue:" .. queueName .. ":reject_count:" .. taskId
				local rejectCount = tonumber(redis.call('GET', rejectCountKey)) or 0
				return {taskId, taskPartition, payload or "", tostring(rejectCount)}
			end
		else
			redis.call('ZREM', prioritiesKey, maxPriority)
		end
	end

	if not found then
		local partitionsKey = "queue:" .. queueName .. ":partitions"
		redis.call('SREM', partitionsKey, partition)
	end
    
    return nil
end

-- Основной цикл: набираем до prefetchCount задач (4 элемента на задачу: id, partition, payload, rejectCount)
while #results / 4 < prefetchCount do
	local found = false
	local partitions = redis.call('SMEMBERS', partitionsKey)
	for i = 1, #partitions do
		local taskData = getFromPartition(partitions[i])
		if taskData then
            found = true
			results[#results + 1] = taskData[1]
			results[#results + 1] = taskData[2]
			results[#results + 1] = taskData[3]
			results[#results + 1] = taskData[4]
			break
		end
	end

	if not found then
		break
	end
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
var ackScript = redis.NewScript(`
local queueName = ARGV[1]
local taskId = ARGV[2]
local consumerId = ARGV[3]
local idempotencyTtl = tonumber(ARGV[4])

-- Формируем ключи
local payloadKey = "queue:" .. queueName .. ":payload:" .. taskId
local partitionKey = "queue:" .. queueName .. ":partition:" .. taskId
local priorityKey = "queue:" .. queueName .. ":priority:" .. taskId

-- Проверяем что задача в processing и принадлежит этому консьюмеру
local consumerTasksKey = "queue:" .. queueName .. ":consumer:" .. consumerId .. ":tasks"

local processingAt = redis.call('HGET', consumerTasksKey, taskId)
if processingAt == false then
    return 0
end

-- Получаем партицию
local partitionCode = redis.call('GET', partitionKey) or "base"
local priority = redis.call('GET', priorityKey) or 0
local consumerPartitionCountKey = "queue:" .. queueName .. ":consumer:" .. consumerId .. ":partition:" .. partitionCode .. ":count"
local consumerPartitionCount = redis.call('DECR', consumerPartitionCountKey) or 0
if consumerPartitionCount <= 0 then
	redis.call('DEL', consumerPartitionCountKey)
end

-- Разблокируем партицию только если она с префиксом "!" (заблокированная)
if partitionCode:sub(1, 1) == "!" and consumerPartitionCount == 0 then
    local partitionLockKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":lock"
    local lockOwner = redis.call('GET', partitionLockKey)
    if lockOwner == consumerId then
        redis.call('DEL', partitionLockKey)
    end
end

-- Удаляем payload, ключ партиции и reject_count
if idempotencyTtl > 0 then
	redis.call('SET', payloadKey, '0')
	redis.call('EXPIRE', payloadKey, idempotencyTtl)
else
	redis.call('DEL', payloadKey)
end
redis.call('DEL', partitionKey)
redis.call('DEL', priorityKey)
redis.call('DEL', "queue:" .. queueName .. ":reject_count:" .. taskId)

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

-- Удаляем задачу из hash консьюмера
redis.call('HDEL', consumerTasksKey, taskId)

return 1
`)

// getRejectScript возвращает Lua скрипт для отклонения задачи.
// Для ordered-партиций (префикс "!"): priority+1, задача в конец очереди с новым приоритетом —
// при Get сначала берутся задачи с большим приоритетом, порядок сохраняется.
// При waitTime > 0 для ordered-партиций: :block = unlockAt(ms), TTL = ceil(waitTime) сек.
// ARGV[1] = queue name
// ARGV[2] = task ID
// ARGV[3] = consumer ID
// ARGV[4] = waitTime в секундах (дробное OK; 0 = без блокировки)
var rejectScript = redis.NewScript(`
local queueName = ARGV[1]
local taskId = ARGV[2]
local consumerId = ARGV[3]
local waitTime = tonumber(ARGV[4]) or 0

-- Формируем ключи
local queueKey = "queue:" .. queueName
local consumerTasksKey = "queue:" .. queueName .. ":consumer:" .. consumerId .. ":tasks"

-- Проверяем что задача в processing и принадлежит этому консьюмеру
local processingAt = redis.call('HGET', consumerTasksKey, taskId)
if processingAt == false then
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

-- Используем текущее время для возврата задачи
local now = redis.call('TIME')
local nowMs = now[1] * 1000 + math.floor(now[2] / 1000)
-- Чистое время для :block (без tie-break reject_seq)
local blockNowMs = nowMs

local newPriority = priority
local needsLockUnlock = partitionCode:sub(1, 1) == "!"

-- Инкрементируем счётчик reject для расчёта задержки при следующих попытках
local rejectCountKey = "queue:" .. queueName .. ":reject_count:" .. taskId
redis.call('INCR', rejectCountKey)

if needsLockUnlock then
    -- Ordered partition: увеличиваем приоритет, задача попадёт в конец "reject"-очереди
    -- и будет обработана первой при следующем Get (приоритеты по убыванию)
    newPriority = priority + 1
    redis.call('SET', priorityKey, tostring(newPriority))
    -- Тай-брейк для порядка при нескольких reject в одну миллисекунду
    local seq = redis.call('INCR', "queue:" .. queueName .. ":reject_seq")
    nowMs = nowMs + seq * 0.000001
end

-- Возвращаем задачу обратно в очередь
local partitionQueueKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":" .. newPriority
redis.call('ZADD', partitionQueueKey, nowMs, taskId)
local partitionsKey = "queue:" .. queueName .. ":partitions"
redis.call('SADD', partitionsKey, partitionCode)
local prioritiesKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":priorities"
redis.call('ZADD', prioritiesKey, newPriority, tostring(newPriority))

local consumerPartitionCountKey = "queue:" .. queueName .. ":consumer:" .. consumerId .. ":partition:" .. partitionCode .. ":count"
local consumerPartitionCount = redis.call('DECR', consumerPartitionCountKey) or 0
if consumerPartitionCount <= 0 then
	redis.call('DEL', consumerPartitionCountKey)
end

-- Разблокируем партицию только если она с префиксом "!"
if needsLockUnlock and consumerPartitionCount == 0 then
    local partitionLockKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":lock"
    local lockOwner = redis.call('GET', partitionLockKey)
    if lockOwner == consumerId then
        redis.call('DEL', partitionLockKey)
    end
end

-- При waitTime > 0 ставим блок: unlockAt = now + waitTime*1000; TTL = ceil(waitTime)
if needsLockUnlock and waitTime > 0 then
	local partitionBlockKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":block"
	local ttlSec = math.ceil(waitTime)
	if ttlSec < 1 then
		ttlSec = 1
	end
	local unlockAt = math.floor(blockNowMs + waitTime * 1000)
	redis.call('SET', partitionBlockKey, tostring(unlockAt), 'EX', ttlSec)
end

-- Удаляем задачу из hash консьюмера (prefetch)
redis.call('HDEL', consumerTasksKey, taskId)

return 1
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

-- Получаем список всех консамеров
local consumers = redis.call('SMEMBERS', consumersKey)
for i = 1, #consumers do
    local lockOwner = consumers[i]

	-- Проверяем что консьюмер жив
	local lockOwnerKey = "queue:" .. queueName .. ":consumer:" .. lockOwner
	local lastPing = redis.call('GET', lockOwnerKey)

	if lastPing == false then
		 -- Консьюмер мертв - разблокируем партицию и возвращаем задачу
		local lockOwnerTasksKey = "queue:" .. queueName .. ":consumer:" .. lockOwner .. ":tasks"
		local tasksData = redis.call('HGETALL', lockOwnerTasksKey)
		
		-- Ищем задачу из этой партиции в hash (HGETALL возвращает {key1, val1, key2, val2, ...})
		for j = 1, #tasksData, 2 do
			local taskToReturn = tasksData[j]
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

				local newTaskPriority = taskPriority
				local needsLockUnlock = taskPartition:sub(1, 1) == "!"

				if needsLockUnlock then
					-- Ordered partition: увеличиваем приоритет, задача попадёт в конец "reject"-очереди
					-- и будет обработана первой при следующем Get (приоритеты по убыванию)
					newTaskPriority = taskPriority + 1
					redis.call('SET', taskPriorityKey, tostring(newTaskPriority))
					-- Тай-брейк для порядка при нескольких reject в одну миллисекунду
					local seq = redis.call('INCR', "queue:" .. queueName .. ":reject_seq")
					now = now + seq * 0.000001
				end

				-- Разблокируем партицию только если она с префиксом "!"
				if taskPartition:sub(1, 1) == "!" then
					local partitionLockKey = "queue:" .. queueName .. ":partition:" .. taskPartition .. ":lock"
					redis.call('DEL', partitionLockKey)
				end

				redis.call('HDEL', lockOwnerTasksKey, taskToReturn)

				local partitionQueueKey = "queue:" .. queueName .. ":partition:" .. taskPartition .. ":" .. newTaskPriority
				redis.call('ZADD', partitionQueueKey, now, taskToReturn)
				local partitionsKey = "queue:" .. queueName .. ":partitions"
				redis.call('SADD', partitionsKey, taskPartition)
				local prioritiesKey = "queue:" .. queueName .. ":partition:" .. taskPartition .. ":priorities"
				redis.call('ZADD', prioritiesKey, newTaskPriority, tostring(newTaskPriority))

				break
			end
		end
		
		-- Очищаем мусор от мертвого консьюмера (все оставшиеся задачи обработаем ниже)
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
redis.call('DEL', payloadKey, partitionKey, priorityKey, "queue:" .. queueName .. ":reject_count:" .. taskId)
cleanupTags()

local prioritiesKey = "queue:" .. queueName .. ":partition:" .. partition .. ":priorities"
if redis.call('ZCARD', queueKey) == 0 then
	redis.call('ZREM', prioritiesKey, priority)
	if redis.call('ZCARD', prioritiesKey) == 0 then
		redis.call('SREM', "queue:" .. queueName .. ":partitions", partition)
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
	redis.call('DEL', payloadKey, partitionKey, priorityKey, "queue:" .. queueName .. ":reject_count:" .. taskId)
	cleanupTags(taskId)

	local prioritiesKey = "queue:" .. queueName .. ":partition:" .. partition .. ":priorities"
	if redis.call('ZCARD', queueKey) == 0 then
		redis.call('ZREM', prioritiesKey, priority)
		if redis.call('ZCARD', prioritiesKey) == 0 then
			redis.call('SREM', "queue:" .. queueName .. ":partitions", partition)
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

func getPingScript() *redis.Script {
	return pingScript
}

func getRemoveScript() *redis.Script {
	return removeScript
}

func getRemoveByTagScript() *redis.Script {
	return removeByTagScript
}
