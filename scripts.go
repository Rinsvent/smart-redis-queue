package redisqueue

import (
	"github.com/redis/go-redis/v9"
)

// getAddScript возвращает Lua скрипт для добавления задач (батч)
// ARGV[1] = queue name
// ARGV[2] = количество задач
// Далее группы по 5 аргумента на задачу:
//
//	ARGV[3 + i*5] = task ID
//	ARGV[4 + i*5] = partition code (пустая строка = без партиции)
//	ARGV[4 + i*5] = priority
//	ARGV[5 + i*5] = scheduled timestamp (ms)
//	ARGV[6 + i*5] = payload
//
// Возвращает количество успешно добавленных задач
var addScript = redis.NewScript(`
local queueName = ARGV[1]
local taskCount = tonumber(ARGV[2])

local partitionsKey = "queue:" .. queueName .. ":partitions"

local added = 0

for i = 0, taskCount - 1 do
    local base = 3 + i * 5
    local taskId = ARGV[base]
    local partitionCode = ARGV[base + 1]
	if partitionCode == "" then
        partitionCode = "base"
	end
    local priority = ARGV[base + 2]
    local scheduled = tonumber(ARGV[base + 3])
    local payload = ARGV[base + 4]

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

        added = added + 1
    end
end

return added
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

-- Регистрируем/обновляем консьюмера
redis.call('SADD', consumersKey, consumerId)
redis.call('SET', consumerKey, currentUnix, 'EX', 120)

local results = {}

-- Функция для получения одной задачи из партиции
-- Партиции с префиксом "!" блокируются (эксклюзивны для одного консьюмера).
-- Тот же консьюмер может брать несколько задач подряд; блокировка снимается
-- только когда все задачи из партиции акнуты или реджекнуты.
-- Ключ :block с TTL блокирует партицию до истечения (Reject с waitTime при ratelimit).
local function getFromPartition(partition)
    local needsLock = partition:sub(1, 1) == "!"
    
    if needsLock then
        -- Проверяем TTL-блок (ratelimit cooldown)
        local partitionBlockKey = "queue:" .. queueName .. ":partition:" .. partition .. ":block"
        if redis.call('EXISTS', partitionBlockKey) == 1 then
            return nil
        end
        local partitionLockKey = "queue:" .. queueName .. ":partition:" .. partition .. ":lock"
        local lockOwner = redis.call('GET', partitionLockKey)
        if lockOwner == false then
            local lockAcquired = redis.call('SET', partitionLockKey, consumerId, 'NX')
            if not lockAcquired then
                return nil
            end
        elseif lockOwner ~= consumerId then
            return nil
        end
    end

	local consumerPartitionCountKey = "queue:" .. queueName .. ":consumer:" .. consumerId .. ":partition" .. partition .. ":count"
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
local consumerPartitionCountKey = "queue:" .. queueName .. ":consumer:" .. consumerId .. ":partition" .. partitionCode .. ":count"
local consumerPartitionCount = redis.call('DECR', consumerPartitionCountKey) or 0

-- Разблокируем партицию только если она с префиксом "!" (заблокированная)
if partitionCode:sub(1, 1) == "!" and consumerPartitionCount == 0 then
    local partitionLockKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":lock"
    local lockOwner = redis.call('GET', partitionLockKey)
    if lockOwner == consumerId then
        redis.call('DEL', partitionLockKey)
    end
end

-- Удаляем payload, ключ партиции и reject_count
redis.call('DEL', payloadKey)
redis.call('DEL', partitionKey)
redis.call('DEL', priorityKey)
redis.call('DEL', "queue:" .. queueName .. ":reject_count:" .. taskId)

-- Удаляем задачу из hash консьюмера
redis.call('HDEL', consumerTasksKey, taskId)

return 1
`)

// getRejectScript возвращает Lua скрипт для отклонения задачи.
// Для ordered-партиций (префикс "!"): priority+1, задача в конец очереди с новым приоритетом —
// при Get сначала берутся задачи с большим приоритетом, порядок сохраняется.
// При waitTime > 0 для ordered-партиций ставится TTL-блок: партиция не берётся до истечения (ratelimit).
// ARGV[1] = queue name
// ARGV[2] = task ID
// ARGV[3] = consumer ID
// ARGV[4] = waitTime в секундах (0 = без блокировки)
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

local consumerPartitionCountKey = "queue:" .. queueName .. ":consumer:" .. consumerId .. ":partition" .. partitionCode .. ":count"
local consumerPartitionCount = redis.call('DECR', consumerPartitionCountKey) or 0

-- Разблокируем партицию только если она с префиксом "!"
if needsLockUnlock and consumerPartitionCount == 0 then
    local partitionLockKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":lock"
    local lockOwner = redis.call('GET', partitionLockKey)
    if lockOwner == consumerId then
        redis.call('DEL', partitionLockKey)
    end
end

-- При waitTime > 0 ставим TTL-блок: партиция не берётся до истечения (ratelimit)
if needsLockUnlock and waitTime > 0 then
	local partitionBlockKey = "queue:" .. queueName .. ":partition:" .. partitionCode .. ":block"
	redis.call('SET', partitionBlockKey, '1', 'EX', waitTime)
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
local consumersKey = "queue:" .. queueName .. ":consumers"
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

// Функции для получения скриптов

func getAddScript() *redis.Script {
	return addScript
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
