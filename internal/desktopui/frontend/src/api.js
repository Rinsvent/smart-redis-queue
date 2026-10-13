function bridge() {
  return window?.go?.desktopui?.App
}

async function call(name, ...args) {
  const app = bridge()
  if (!app || typeof app[name] !== 'function') {
    throw new Error(`Wails binding missing: ${name}`)
  }
  return app[name](...args)
}

export const api = {
  listConnections: () => call('ListConnections'),
  activeConnectionID: () => call('ActiveConnectionID'),
  addConnection: (name, addr, password, db) => call('AddConnection', name, addr, password, db),
  removeConnection: (id) => call('RemoveConnection', id),
  selectConnection: (id) => call('SelectConnection', id),
  testConnection: (addr, password, db) => call('TestConnection', addr, password, db),
  ensureActive: () => call('EnsureActive'),
  listQueues: () => call('ListQueues'),
  purge: (queue, partition) => call('Purge', queue, partition || ''),
  retry: (queue) => call('Retry', queue),
  getTaskPayload: (queue, taskId) => call('GetTaskPayload', queue, taskId),
  countByTag: (queue, tag) => call('CountByTag', queue, tag),
  removeByTag: (queue, tag, limit, returnPayload) =>
    call('RemoveByTag', queue, tag, limit, returnPayload),
}
