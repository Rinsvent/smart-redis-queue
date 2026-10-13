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
  addConnection: (name, addr, password) => call('AddConnection', name, addr, password),
  removeConnection: (id) => call('RemoveConnection', id),
  selectConnection: (id) => call('SelectConnection', id),
  testConnection: (addr, password) => call('TestConnection', addr, password),
  ensureActive: () => call('EnsureActive'),
  refreshCatalog: () => call('RefreshCatalog'),
  listQueues: () => call('ListQueues'),
  inspectQueue: (name, db) => call('InspectQueue', name, db),
  purge: (queue, partition, db) => call('Purge', queue, partition || '', db),
  retry: (queue, db) => call('Retry', queue, db),
  getTaskPayload: (queue, taskId, db) => call('GetTaskPayload', queue, taskId, db),
  countByTag: (queue, tag, db) => call('CountByTag', queue, tag, db),
  removeByTag: (queue, tag, limit, returnPayload, db) =>
    call('RemoveByTag', queue, tag, limit, returnPayload, db),
}
