import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  Alert,
  AppBar,
  Box,
  Button,
  ButtonBase,
  Checkbox,
  CircularProgress,
  CssBaseline,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  FormControlLabel,
  IconButton,
  InputLabel,
  ListItemIcon,
  ListItemText,
  Menu,
  MenuItem,
  Select,
  Snackbar,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  ThemeProvider,
  Toolbar,
  Tooltip,
  Typography,
  createTheme,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import BlockIcon from '@mui/icons-material/Block'
import CheckIcon from '@mui/icons-material/Check'
import DarkModeOutlinedIcon from '@mui/icons-material/DarkModeOutlined'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline'
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown'
import LightModeOutlinedIcon from '@mui/icons-material/LightModeOutlined'
import LockOpenIcon from '@mui/icons-material/LockOpen'
import MoreVertIcon from '@mui/icons-material/MoreVert'
import RefreshIcon from '@mui/icons-material/Refresh'
import { api } from './api'

const FONT = '"DM Sans", "Segoe UI", sans-serif'

const VIEW_MODES = [
  { id: 'partitions', label: 'Partitions' },
  { id: 'groups', label: 'Groups' },
  { id: 'tags', label: 'Tags' },
]

const POLL_PRESETS = [
  { label: 'Off', ms: 0 },
  { label: '5s', ms: 5000 },
  { label: '10s', ms: 10000 },
  { label: '30s', ms: 30000 },
  { label: '1m', ms: 60000 },
  { label: '5m', ms: 300000 },
]

function buildTheme(mode) {
  const dark = mode === 'dark'
  return createTheme({
    palette: {
      mode,
      primary: { main: dark ? '#5B9BD5' : '#1F6AA5' },
      background: {
        default: dark ? '#12151A' : '#EEF1F5',
        paper: dark ? '#1A1F27' : '#FFFFFF',
      },
      divider: dark ? '#2A3140' : '#D5DCE6',
    },
    typography: {
      fontFamily: FONT,
      h4: { fontWeight: 700, letterSpacing: '-0.02em' },
      h6: { fontWeight: 600 },
      button: { textTransform: 'none', fontWeight: 600 },
    },
    shape: { borderRadius: 10 },
    components: {
      MuiCssBaseline: {
        styleOverrides: {
          body: {
            minHeight: '100vh',
            backgroundImage: dark
              ? 'radial-gradient(ellipse at top left, #1a2433 0%, #12151A 55%)'
              : 'radial-gradient(ellipse at top left, #e8eef6 0%, #EEF1F5 55%)',
          },
        },
      },
      MuiAppBar: {
        styleOverrides: {
          root: {
            backgroundImage: 'none',
            borderBottom: `1px solid ${dark ? '#2A3140' : '#D5DCE6'}`,
          },
        },
      },
    },
  })
}

function rateLabel(r) {
  if (!r) return '—'
  const p = Number(r.processedPerSec || 0).toFixed(1)
  const b = Number(r.backlogDeltaPerSec || 0)
  const sign = b > 0 ? '+' : ''
  return `~${p} msg/s processed · backlog ${sign}${b.toFixed(1)}/s`
}

function formatUnlockAt(score) {
  const ms = Number(score)
  if (!Number.isFinite(ms) || ms <= 0) return '—'
  try {
    return new Date(ms).toLocaleString()
  } catch {
    return String(ms)
  }
}

function readPollMs() {
  const n = Number(localStorage.getItem('srq-poll-ms'))
  if (Number.isFinite(n) && POLL_PRESETS.some((p) => p.ms === n)) return n
  return 5000
}

export default function App() {
  const [themeMode, setThemeMode] = useState(() => localStorage.getItem('srq-theme') || 'light')
  const theme = useMemo(() => buildTheme(themeMode), [themeMode])

  const [connections, setConnections] = useState([])
  const [activeId, setActiveId] = useState('')
  const [queues, setQueues] = useState([])
  const [queueFilter, setQueueFilter] = useState('')
  const [selectedQueue, setSelectedQueue] = useState('')
  const [activeDb, setActiveDb] = useState(0)
  const [detail, setDetail] = useState(null)

  const [viewMode, setViewMode] = useState('partitions')
  const [rows, setRows] = useState([])
  const [listTotal, setListTotal] = useState(0)
  const [listTruncated, setListTruncated] = useState(false)
  const [listFilter, setListFilter] = useState('')
  const [listFilterDraft, setListFilterDraft] = useState('')
  const [loading, setLoading] = useState(false)
  const [catalogLoading, setCatalogLoading] = useState(false)
  const [listLoading, setListLoading] = useState(false)
  const [pollMs, setPollMs] = useState(readPollMs)
  const [snack, setSnack] = useState({ open: false, msg: '', severity: 'info' })

  const [addOpen, setAddOpen] = useState(false)
  const [form, setForm] = useState({ name: '', addr: 'localhost:6379', password: '' })
  const [testing, setTesting] = useState(false)

  const [payloadOpen, setPayloadOpen] = useState(false)
  const [payloadId, setPayloadId] = useState('')
  const [payloadResult, setPayloadResult] = useState(null)

  const [tagOpen, setTagOpen] = useState(false)
  const [tagMode, setTagMode] = useState('count')
  const [tag, setTag] = useState('')
  const [tagLimit, setTagLimit] = useState(100)
  const [tagPreview, setTagPreview] = useState(false)
  const [tagResult, setTagResult] = useState(null)

  const [blockOpen, setBlockOpen] = useState(false)
  const [blockPartition, setBlockPartition] = useState('')
  const [blockSecs, setBlockSecs] = useState('60')

  const [confirm, setConfirm] = useState(null)
  const [actionsAnchor, setActionsAnchor] = useState(null)
  const [pollAnchor, setPollAnchor] = useState(null)
  const pollRef = useRef(null)
  const filterTimer = useRef(null)

  const notify = (msg, severity = 'info') => setSnack({ open: true, msg, severity })

  const filteredQueues = useMemo(() => {
    const q = queueFilter.trim().toLowerCase()
    if (!q) return queues
    return queues.filter((item) => item.name.toLowerCase().includes(q))
  }, [queues, queueFilter])

  const pollLabel = POLL_PRESETS.find((p) => p.ms === pollMs)?.label || 'Off'

  const filterPlaceholder =
    viewMode === 'groups' ? 'Filter groups' : viewMode === 'tags' ? 'Filter tags' : 'Filter partitions'

  const loadConnections = useCallback(async () => {
    try {
      const list = await api.listConnections()
      setConnections(list || [])
      const id = await api.activeConnectionID()
      setActiveId(id || '')
    } catch (e) {
      notify(String(e), 'error')
    }
  }, [])

  const loadQueues = useCallback(async () => {
    if (!activeId) {
      setQueues([])
      return
    }
    setLoading(true)
    try {
      await api.ensureActive()
      const list = await api.listQueues()
      setQueues(list || [])
    } catch (e) {
      notify(String(e), 'error')
      setQueues([])
    } finally {
      setLoading(false)
    }
  }, [activeId])

  const refreshCatalog = useCallback(async () => {
    if (!activeId) return
    setCatalogLoading(true)
    try {
      await api.ensureActive()
      await api.refreshCatalog()
      const list = await api.listQueues()
      setQueues(list || [])
      notify('Catalog refreshed', 'success')
    } catch (e) {
      notify(String(e), 'error')
    } finally {
      setCatalogLoading(false)
    }
  }, [activeId])

  const loadDetail = useCallback(async () => {
    if (!selectedQueue) {
      setDetail(null)
      return
    }
    try {
      const snap = await api.inspectQueue(selectedQueue, activeDb)
      setDetail(snap)
    } catch (e) {
      notify(String(e), 'error')
      setDetail(null)
    }
  }, [selectedQueue, activeDb])

  const loadList = useCallback(async () => {
    if (!selectedQueue) {
      setRows([])
      setListTotal(0)
      setListTruncated(false)
      return
    }
    setListLoading(true)
    try {
      if (viewMode === 'groups') {
        const r = await api.listGroups(selectedQueue, activeDb, listFilter)
        setRows(r?.groups || [])
        setListTotal(r?.total || 0)
        setListTruncated(!!r?.truncated)
      } else if (viewMode === 'tags') {
        const r = await api.listTags(selectedQueue, activeDb, listFilter)
        setRows(r?.tags || [])
        setListTotal(r?.total || 0)
        setListTruncated(!!r?.truncated)
      } else {
        const r = await api.listPartitions(selectedQueue, activeDb, listFilter)
        setRows(r?.partitions || [])
        setListTotal(r?.total || 0)
        setListTruncated(!!r?.truncated)
      }
    } catch (e) {
      notify(String(e), 'error')
      setRows([])
      setListTotal(0)
      setListTruncated(false)
    } finally {
      setListLoading(false)
    }
  }, [selectedQueue, activeDb, listFilter, viewMode])

  const refreshSelected = useCallback(() => {
    loadDetail()
    loadList()
  }, [loadDetail, loadList])

  useEffect(() => {
    loadConnections()
  }, [loadConnections])

  useEffect(() => {
    loadQueues()
  }, [loadQueues])

  useEffect(() => {
    const item = queues.find((q) => q.name === selectedQueue)
    if (!item) {
      setDetail(null)
      setRows([])
      return undefined
    }
    if (!item.dbs?.includes(activeDb)) {
      setActiveDb(item.dbs?.[0] ?? 0)
      return undefined
    }
    loadDetail()
    loadList()
    if (pollRef.current) clearInterval(pollRef.current)
    if (pollMs > 0) {
      pollRef.current = setInterval(() => {
        loadDetail()
        loadList()
      }, pollMs)
    }
    return () => {
      if (pollRef.current) clearInterval(pollRef.current)
    }
  }, [queues, selectedQueue, activeDb, loadDetail, loadList, pollMs])

  useEffect(() => {
    localStorage.setItem('srq-theme', themeMode)
  }, [themeMode])

  useEffect(() => {
    localStorage.setItem('srq-poll-ms', String(pollMs))
  }, [pollMs])

  useEffect(() => {
    if (filterTimer.current) clearTimeout(filterTimer.current)
    filterTimer.current = setTimeout(() => {
      setListFilter(listFilterDraft.trim())
    }, 300)
    return () => {
      if (filterTimer.current) clearTimeout(filterTimer.current)
    }
  }, [listFilterDraft])

  const selectedMeta = queues.find((q) => q.name === selectedQueue) || null

  const onSelectQueue = (name) => {
    const item = queues.find((q) => q.name === name)
    setSelectedQueue(name)
    setActiveDb(item?.dbs?.[0] ?? 0)
    setListFilterDraft('')
    setListFilter('')
  }

  const onSelectConn = async (id) => {
    try {
      await api.selectConnection(id)
      setActiveId(id)
      setSelectedQueue('')
      setDetail(null)
      notify('Connection selected')
    } catch (e) {
      notify(String(e), 'error')
    }
  }

  const onAdd = async () => {
    setTesting(true)
    try {
      await api.testConnection(form.addr, form.password)
      const c = await api.addConnection(form.name, form.addr, form.password)
      setAddOpen(false)
      setForm({ name: '', addr: 'localhost:6379', password: '' })
      await loadConnections()
      await onSelectConn(c.id)
      notify('Connection saved', 'success')
    } catch (e) {
      notify(String(e), 'error')
    } finally {
      setTesting(false)
    }
  }

  const onDeleteConn = () => {
    if (!activeId) return
    setConfirm({
      title: 'Remove connection?',
      body: 'The profile will be removed from the list. Redis data is not affected.',
      action: async () => {
        try {
          await api.removeConnection(activeId)
          setSelectedQueue('')
          setDetail(null)
          await loadConnections()
          notify('Removed')
        } catch (e) {
          notify(String(e), 'error')
        }
      },
    })
  }

  const runPurge = (partition) => {
    if (!selectedQueue) return
    setActionsAnchor(null)
    setConfirm({
      title: partition ? `Purge partition ${partition}?` : `Purge queue ${selectedQueue}?`,
      body: 'All pending tasks on the selected DB will be deleted.',
      action: async () => {
        try {
          const n = await api.purge(selectedQueue, partition || '', activeDb)
          notify(`Deleted tasks: ${n}`, 'success')
          await refreshCatalog()
          refreshSelected()
        } catch (e) {
          notify(String(e), 'error')
        }
      },
    })
  }

  const openBlock = (partition) => {
    setBlockPartition(partition)
    setBlockSecs('60')
    setBlockOpen(true)
  }

  const runBlock = async () => {
    const secs = Number(blockSecs)
    if (!selectedQueue || !blockPartition || !Number.isFinite(secs) || secs <= 0) {
      notify('Duration must be > 0 seconds', 'error')
      return
    }
    try {
      await api.blockPartition(selectedQueue, blockPartition, secs, activeDb)
      setBlockOpen(false)
      notify(`Blocked ${blockPartition} for ${secs}s`, 'success')
      refreshSelected()
    } catch (e) {
      notify(String(e), 'error')
    }
  }

  const runUnblock = (partition) => {
    if (!selectedQueue) return
    setConfirm({
      title: `Unblock ${partition}?`,
      body: 'Workers will be able to pick tasks from this partition again.',
      action: async () => {
        try {
          await api.unblockPartition(selectedQueue, partition, activeDb)
          notify(`Unblocked ${partition}`, 'success')
          refreshSelected()
        } catch (e) {
          notify(String(e), 'error')
        }
      },
    })
  }

  const runRetry = () => {
    if (!selectedQueue) return
    setActionsAnchor(null)
    setConfirm({
      title: `Retry ${selectedQueue}?`,
      body: 'In-progress tasks will return to pending.',
      action: async () => {
        try {
          const n = await api.retry(selectedQueue, activeDb)
          notify(`Returned: ${n}`, 'success')
          refreshSelected()
        } catch (e) {
          notify(String(e), 'error')
        }
      },
    })
  }

  const loadPayload = async () => {
    if (!selectedQueue || !payloadId.trim()) return
    try {
      const r = await api.getTaskPayload(selectedQueue, payloadId.trim(), activeDb)
      setPayloadResult(r)
    } catch (e) {
      setPayloadResult(null)
      notify(String(e), 'error')
    }
  }

  const runTag = async () => {
    if (!selectedQueue || !tag.trim()) return
    try {
      if (tagMode === 'count') {
        const n = await api.countByTag(selectedQueue, tag.trim(), activeDb)
        setTagResult({ count: n })
      } else {
        const r = await api.removeByTag(
          selectedQueue,
          tag.trim(),
          Number(tagLimit) || 0,
          tagPreview,
          activeDb,
        )
        setTagResult(r)
        refreshSelected()
      }
    } catch (e) {
      notify(String(e), 'error')
    }
  }

  const runRemoveTag = (tagName) => {
    if (!selectedQueue || !tagName) return
    setConfirm({
      title: `Remove by tag ${tagName}?`,
      body: 'Pending tasks with this tag will be removed (up to 1000 per call).',
      action: async () => {
        try {
          const r = await api.removeByTag(selectedQueue, tagName, 1000, false, activeDb)
          notify(`Removed: ${r?.removed ?? 0}`, 'success')
          refreshSelected()
        } catch (e) {
          notify(String(e), 'error')
        }
      },
    })
  }

  const countLabel = listTruncated ? `${rows.length}/${listTotal}` : String(listTotal)
  const countTitle = listTruncated
    ? `Showing ${rows.length} of ${listTotal}`
    : `${listTotal} matches`

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ height: '100vh', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        <AppBar position="sticky" color="transparent" elevation={0} sx={{ flexShrink: 0 }}>
          <Toolbar sx={{ gap: 2, flexWrap: 'wrap' }}>
            <Typography variant="h6" sx={{ mr: 1, color: 'text.primary' }}>
              Smart Redis Queue
            </Typography>
            {connections.length > 0 && (
              <>
                <FormControl size="small" sx={{ minWidth: 220 }}>
                  <InputLabel>Host</InputLabel>
                  <Select
                    label="Host"
                    value={activeId || ''}
                    onChange={(e) => onSelectConn(e.target.value)}
                  >
                    {connections.map((c) => (
                      <MenuItem key={c.id} value={c.id}>
                        {c.name} ({c.addr})
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
                <IconButton onClick={() => setAddOpen(true)} title="Add">
                  <AddIcon />
                </IconButton>
                <IconButton onClick={onDeleteConn} disabled={!activeId} title="Remove">
                  <DeleteOutlineIcon />
                </IconButton>
                <IconButton
                  onClick={refreshCatalog}
                  disabled={!activeId || catalogLoading}
                  title="Refresh catalog (all DBs)"
                >
                  {catalogLoading ? <CircularProgress size={18} /> : <RefreshIcon />}
                </IconButton>
              </>
            )}
            <Box sx={{ flexGrow: 1 }} />
            <IconButton
              onClick={() => setThemeMode((m) => (m === 'light' ? 'dark' : 'light'))}
              title="Theme"
            >
              {themeMode === 'light' ? <DarkModeOutlinedIcon /> : <LightModeOutlinedIcon />}
            </IconButton>
          </Toolbar>
        </AppBar>

        {connections.length === 0 ? (
          <Box
            sx={{
              flex: 1,
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
              gap: 2,
              px: 3,
            }}
          >
            <Typography variant="h4" color="text.primary">
              Smart Redis Queue
            </Typography>
            <Typography color="text.secondary" sx={{ maxWidth: 420, textAlign: 'center' }}>
              Add a Redis host — queues are discovered across all databases automatically.
            </Typography>
            <Button
              variant="contained"
              size="large"
              startIcon={<AddIcon />}
              onClick={() => setAddOpen(true)}
              sx={{ mt: 1, px: 4, py: 1.5, fontSize: '1.05rem' }}
            >
              Add connection
            </Button>
          </Box>
        ) : (
          <Box
            sx={{
              flex: 1,
              minHeight: 0,
              display: 'grid',
              gridTemplateColumns: { xs: '1fr', md: '260px 1fr' },
              gap: 0,
            }}
          >
            <Box
              sx={{
                borderRight: 1,
                borderColor: 'divider',
                p: 1.5,
                display: 'flex',
                flexDirection: 'column',
                minHeight: 0,
                bgcolor: 'background.paper',
              }}
            >
              <Stack direction="row" alignItems="center" spacing={1} mb={1}>
                <TextField
                  size="small"
                  fullWidth
                  placeholder="Queues"
                  value={queueFilter}
                  onChange={(e) => setQueueFilter(e.target.value)}
                />
                {loading && <CircularProgress size={16} />}
              </Stack>
              <Box sx={{ flex: 1, minHeight: 0, overflow: 'auto' }}>
                {filteredQueues.length === 0 ? (
                  <Typography variant="body2" color="text.secondary" sx={{ px: 1 }}>
                    No queues
                  </Typography>
                ) : (
                  <Stack spacing={0}>
                    {filteredQueues.map((q) => {
                      const active = selectedQueue === q.name
                      return (
                        <Box
                          key={q.name}
                          onClick={() => onSelectQueue(q.name)}
                          sx={{
                            px: 1.25,
                            py: 0.65,
                            cursor: 'pointer',
                            borderRadius: 1,
                            bgcolor: active ? 'action.selected' : 'transparent',
                            '&:hover': { bgcolor: 'action.hover' },
                          }}
                        >
                          <Typography
                            noWrap
                            title={q.name}
                            sx={{
                              fontSize: 13.5,
                              fontWeight: active ? 700 : 500,
                              color: active ? 'primary.main' : 'text.primary',
                            }}
                          >
                            {q.name}
                          </Typography>
                        </Box>
                      )
                    })}
                  </Stack>
                )}
              </Box>
            </Box>

            <Box
              sx={{
                p: 2.5,
                display: 'flex',
                flexDirection: 'column',
                minHeight: 0,
                overflow: 'hidden',
              }}
            >
              {!selectedMeta ? (
                <Typography color="text.secondary">Select a queue on the left</Typography>
              ) : (
                <>
                  <Stack spacing={1.5} sx={{ flexShrink: 0 }}>
                    <Stack
                      direction="row"
                      alignItems="center"
                      flexWrap="wrap"
                      useFlexGap
                      spacing={2}
                      columnGap={3}
                      rowGap={1}
                    >
                      <Box sx={{ minWidth: 0, flex: '1 1 160px' }}>
                        <Typography variant="h5" noWrap title={selectedMeta.name}>
                          {selectedMeta.name}
                        </Typography>
                        <Typography color="text.secondary" variant="body2" noWrap>
                          {detail ? rateLabel(detail.rate) : '—'}
                        </Typography>
                      </Box>

                      <Stack direction="row" spacing={2.5} alignItems="center" flexShrink={0}>
                        <Stat label="Pending" value={detail?.pending} />
                        <Stat label="In progress" value={detail?.inProgress} />
                        <Stat label="Consumers" value={detail?.consumers} />
                      </Stack>

                      <Stack
                        direction="row"
                        alignItems="center"
                        spacing={0.75}
                        flexWrap="wrap"
                        useFlexGap
                        sx={{ ml: { md: 'auto' } }}
                      >
                        <Typography variant="body2" color="text.secondary">
                          DB:
                        </Typography>
                        {(selectedMeta.dbs || []).map((db) => {
                          const active = db === activeDb
                          return (
                            <ButtonBase
                              key={db}
                              onClick={() => setActiveDb(db)}
                              sx={{
                                px: 1.25,
                                py: 0.35,
                                borderRadius: 1.5,
                                border: 1,
                                borderColor: active ? 'primary.main' : 'divider',
                                bgcolor: active ? 'primary.main' : 'transparent',
                                color: active ? 'primary.contrastText' : 'text.primary',
                                fontWeight: 600,
                                fontSize: 13,
                                minWidth: 36,
                              }}
                            >
                              {db}
                            </ButtonBase>
                          )
                        })}
                        <IconButton
                          size="small"
                          onClick={(e) => setActionsAnchor(e.currentTarget)}
                          title="Actions"
                        >
                          <MoreVertIcon />
                        </IconButton>
                      </Stack>
                    </Stack>

                    <Stack direction="row" alignItems="center" spacing={1}>
                      <FormControl size="small" sx={{ minWidth: 130, flexShrink: 0 }}>
                        <Select
                          value={viewMode}
                          onChange={(e) => {
                            setViewMode(e.target.value)
                            setListFilterDraft('')
                            setListFilter('')
                            setRows([])
                          }}
                        >
                          {VIEW_MODES.map((m) => (
                            <MenuItem key={m.id} value={m.id}>
                              {m.label}
                            </MenuItem>
                          ))}
                        </Select>
                      </FormControl>
                      <TextField
                        size="small"
                        fullWidth
                        placeholder={filterPlaceholder}
                        value={listFilterDraft}
                        onChange={(e) => setListFilterDraft(e.target.value)}
                      />
                      <Typography
                        variant="body2"
                        color="text.secondary"
                        sx={{ minWidth: 52, textAlign: 'right', flexShrink: 0 }}
                        title={countTitle}
                      >
                        {countLabel}
                      </Typography>
                      <IconButton
                        size="small"
                        onClick={refreshSelected}
                        disabled={listLoading}
                        title="Refresh"
                      >
                        {listLoading ? <CircularProgress size={18} /> : <RefreshIcon fontSize="small" />}
                      </IconButton>
                      <IconButton
                        size="small"
                        onClick={(e) => setPollAnchor(e.currentTarget)}
                        title={`Auto-refresh: ${pollLabel}`}
                        sx={{ ml: -0.5 }}
                      >
                        <KeyboardArrowDownIcon fontSize="small" />
                      </IconButton>
                    </Stack>
                  </Stack>

                  <TableContainer
                    sx={{
                      flex: 1,
                      minHeight: 0,
                      mt: 1.5,
                      border: 1,
                      borderColor: 'divider',
                      borderRadius: 2,
                      bgcolor: 'background.paper',
                    }}
                  >
                    {viewMode === 'partitions' && (
                      <Table size="small" stickyHeader>
                        <TableHead>
                          <TableRow>
                            <TableCell>ID</TableCell>
                            <TableCell align="right">Pending</TableCell>
                            <TableCell align="right">In prog</TableCell>
                            <TableCell>Groups</TableCell>
                            <TableCell>Flags</TableCell>
                            <TableCell align="right">Actions</TableCell>
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {rows.length === 0 ? (
                            <TableRow>
                              <TableCell colSpan={6}>
                                <Typography variant="body2" color="text.secondary">
                                  No partitions found
                                </Typography>
                              </TableCell>
                            </TableRow>
                          ) : (
                            rows.map((p) => (
                              <TableRow key={p.id}>
                                <TableCell sx={{ fontFamily: 'ui-monospace, monospace', fontSize: 12 }}>
                                  {p.id}
                                </TableCell>
                                <TableCell align="right">{p.pending}</TableCell>
                                <TableCell align="right">{p.inProgress}</TableCell>
                                <TableCell>{(p.groups || []).join(', ') || '—'}</TableCell>
                                <TableCell>
                                  {[p.locked && 'locked', p.blocked && 'blocked']
                                    .filter(Boolean)
                                    .join(', ') || '—'}
                                </TableCell>
                                <TableCell align="right">
                                  <Stack direction="row" spacing={0} justifyContent="flex-end">
                                    <Tooltip title="Purge">
                                      <IconButton
                                        size="small"
                                        color="warning"
                                        onClick={() => runPurge(p.id)}
                                      >
                                        <DeleteOutlineIcon fontSize="small" />
                                      </IconButton>
                                    </Tooltip>
                                    <Tooltip title="Block">
                                      <IconButton size="small" onClick={() => openBlock(p.id)}>
                                        <BlockIcon fontSize="small" />
                                      </IconButton>
                                    </Tooltip>
                                    <Tooltip title="Unblock">
                                      <span>
                                        <IconButton
                                          size="small"
                                          disabled={!p.blocked}
                                          onClick={() => runUnblock(p.id)}
                                        >
                                          <LockOpenIcon fontSize="small" />
                                        </IconButton>
                                      </span>
                                    </Tooltip>
                                  </Stack>
                                </TableCell>
                              </TableRow>
                            ))
                          )}
                        </TableBody>
                      </Table>
                    )}

                    {viewMode === 'groups' && (
                      <Table size="small" stickyHeader>
                        <TableHead>
                          <TableRow>
                            <TableCell>Name</TableCell>
                            <TableCell align="right">Ready</TableCell>
                            <TableCell>Blocked</TableCell>
                            <TableCell>Unlock at</TableCell>
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {rows.length === 0 ? (
                            <TableRow>
                              <TableCell colSpan={4}>
                                <Typography variant="body2" color="text.secondary">
                                  No groups found
                                </Typography>
                              </TableCell>
                            </TableRow>
                          ) : (
                            rows.map((g) => (
                              <TableRow key={g.name}>
                                <TableCell sx={{ fontFamily: 'ui-monospace, monospace', fontSize: 12 }}>
                                  {g.name}
                                </TableCell>
                                <TableCell align="right">{g.ready}</TableCell>
                                <TableCell>{g.blocked ? 'yes' : '—'}</TableCell>
                                <TableCell>
                                  {g.blocked ? formatUnlockAt(g.score) : '—'}
                                </TableCell>
                              </TableRow>
                            ))
                          )}
                        </TableBody>
                      </Table>
                    )}

                    {viewMode === 'tags' && (
                      <Table size="small" stickyHeader>
                        <TableHead>
                          <TableRow>
                            <TableCell>Tag</TableCell>
                            <TableCell align="right">Count</TableCell>
                            <TableCell align="right">Actions</TableCell>
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {rows.length === 0 ? (
                            <TableRow>
                              <TableCell colSpan={3}>
                                <Typography variant="body2" color="text.secondary">
                                  No tags found
                                </Typography>
                              </TableCell>
                            </TableRow>
                          ) : (
                            rows.map((t) => (
                              <TableRow key={t.tag}>
                                <TableCell sx={{ fontFamily: 'ui-monospace, monospace', fontSize: 12 }}>
                                  {t.tag}
                                </TableCell>
                                <TableCell align="right">{t.count}</TableCell>
                                <TableCell align="right">
                                  <Tooltip title="Remove by tag">
                                    <IconButton
                                      size="small"
                                      color="warning"
                                      onClick={() => runRemoveTag(t.tag)}
                                    >
                                      <DeleteOutlineIcon fontSize="small" />
                                    </IconButton>
                                  </Tooltip>
                                </TableCell>
                              </TableRow>
                            ))
                          )}
                        </TableBody>
                      </Table>
                    )}
                  </TableContainer>
                </>
              )}
            </Box>
          </Box>
        )}
      </Box>

      <Menu
        anchorEl={actionsAnchor}
        open={!!actionsAnchor}
        onClose={() => setActionsAnchor(null)}
      >
        <MenuItem
          onClick={() => {
            setActionsAnchor(null)
            runPurge('')
          }}
        >
          Purge queue
        </MenuItem>
        <MenuItem onClick={runRetry}>Retry</MenuItem>
        <MenuItem
          onClick={() => {
            setActionsAnchor(null)
            setPayloadResult(null)
            setPayloadId('')
            setPayloadOpen(true)
          }}
        >
          Payload by ID
        </MenuItem>
        <MenuItem
          onClick={() => {
            setActionsAnchor(null)
            setTagMode('count')
            setTagResult(null)
            setTag('')
            setTagOpen(true)
          }}
        >
          Count by tag
        </MenuItem>
        <MenuItem
          onClick={() => {
            setActionsAnchor(null)
            setTagMode('remove')
            setTagResult(null)
            setTag('')
            setTagOpen(true)
          }}
        >
          Remove by tag
        </MenuItem>
      </Menu>

      <Menu anchorEl={pollAnchor} open={!!pollAnchor} onClose={() => setPollAnchor(null)}>
        {POLL_PRESETS.map((p) => (
          <MenuItem
            key={p.label}
            onClick={() => {
              setPollMs(p.ms)
              setPollAnchor(null)
            }}
          >
            <ListItemIcon sx={{ minWidth: 32 }}>
              {pollMs === p.ms ? <CheckIcon fontSize="small" /> : null}
            </ListItemIcon>
            <ListItemText>{p.label}</ListItemText>
          </MenuItem>
        ))}
      </Menu>

      <Dialog open={addOpen} onClose={() => setAddOpen(false)} fullWidth maxWidth="sm">
        <DialogTitle>New Redis connection</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            <TextField
              label="Name"
              value={form.name}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
              autoFocus
            />
            <TextField
              label="Host:port"
              value={form.addr}
              onChange={(e) => setForm({ ...form, addr: e.target.value })}
            />
            <TextField
              label="Password"
              type="password"
              value={form.password}
              onChange={(e) => setForm({ ...form, password: e.target.value })}
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setAddOpen(false)}>Cancel</Button>
          <Button variant="contained" onClick={onAdd} disabled={testing || !form.name || !form.addr}>
            {testing ? 'Testing…' : 'Save'}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={blockOpen} onClose={() => setBlockOpen(false)} fullWidth maxWidth="xs">
        <DialogTitle>Block partition</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            <Typography variant="body2" color="text.secondary">
              {blockPartition}
            </Typography>
            <TextField
              label="Duration (seconds)"
              type="number"
              value={blockSecs}
              onChange={(e) => setBlockSecs(e.target.value)}
              inputProps={{ min: 1, step: 1 }}
              autoFocus
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setBlockOpen(false)}>Cancel</Button>
          <Button variant="contained" onClick={runBlock}>
            Block
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={payloadOpen} onClose={() => setPayloadOpen(false)} fullWidth maxWidth="md">
        <DialogTitle>Payload by ID</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            <Stack direction="row" spacing={1}>
              <TextField
                fullWidth
                label="Task ID"
                value={payloadId}
                onChange={(e) => setPayloadId(e.target.value)}
              />
              <Button variant="contained" onClick={loadPayload}>
                Load
              </Button>
            </Stack>
            {payloadResult && (
              <Box>
                <Typography variant="body2" color="text.secondary">
                  partition={payloadResult.partition} priority={payloadResult.priority} group=
                  {payloadResult.group || '—'}
                </Typography>
                <Box
                  component="pre"
                  sx={{
                    mt: 1,
                    p: 1.5,
                    bgcolor: 'action.hover',
                    borderRadius: 2,
                    overflow: 'auto',
                    maxHeight: 360,
                    fontSize: 13,
                  }}
                >
                  {payloadResult.payload}
                </Box>
              </Box>
            )}
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setPayloadOpen(false)}>Close</Button>
        </DialogActions>
      </Dialog>

      <Dialog open={tagOpen} onClose={() => setTagOpen(false)} fullWidth maxWidth="sm">
        <DialogTitle>{tagMode === 'count' ? 'Count by tag' : 'Remove by tag'}</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            <TextField label="Tag" value={tag} onChange={(e) => setTag(e.target.value)} />
            {tagMode === 'remove' && (
              <>
                <TextField
                  label="Limit (0 = all)"
                  type="number"
                  value={tagLimit}
                  onChange={(e) => setTagLimit(e.target.value)}
                />
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={tagPreview}
                      onChange={(e) => setTagPreview(e.target.checked)}
                    />
                  }
                  label="Return payloads"
                />
              </>
            )}
            {tagResult && (
              <Alert severity="info">
                {tagMode === 'count'
                  ? `Count: ${tagResult.count}`
                  : `Removed: ${tagResult.removed}${
                      tagResult.payloads?.length
                        ? ` · payloads: ${tagResult.payloads.length}`
                        : ''
                    }`}
              </Alert>
            )}
            {tagResult?.payloads?.length > 0 && (
              <Box
                component="pre"
                sx={{
                  p: 1.5,
                  bgcolor: 'action.hover',
                  borderRadius: 2,
                  overflow: 'auto',
                  maxHeight: 240,
                  fontSize: 12,
                }}
              >
                {tagResult.payloads.join('\n---\n')}
              </Box>
            )}
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setTagOpen(false)}>Close</Button>
          <Button variant="contained" onClick={runTag}>
            {tagMode === 'count' ? 'Count' : 'Remove'}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={!!confirm} onClose={() => setConfirm(null)}>
        <DialogTitle>{confirm?.title}</DialogTitle>
        <DialogContent>
          <Typography>{confirm?.body}</Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setConfirm(null)}>Cancel</Button>
          <Button
            color="warning"
            variant="contained"
            onClick={async () => {
              const a = confirm?.action
              setConfirm(null)
              if (a) await a()
            }}
          >
            Confirm
          </Button>
        </DialogActions>
      </Dialog>

      <Snackbar
        open={snack.open}
        autoHideDuration={4000}
        onClose={() => setSnack((s) => ({ ...s, open: false }))}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert
          severity={snack.severity}
          onClose={() => setSnack((s) => ({ ...s, open: false }))}
          variant="filled"
        >
          {snack.msg}
        </Alert>
      </Snackbar>
    </ThemeProvider>
  )
}

function Stat({ label, value }) {
  return (
    <Box sx={{ lineHeight: 1.15 }}>
      <Typography variant="caption" color="text.secondary" display="block">
        {label}
      </Typography>
      <Typography variant="h6" component="div" sx={{ fontWeight: 700 }}>
        {value ?? 0}
      </Typography>
    </Box>
  )
}
