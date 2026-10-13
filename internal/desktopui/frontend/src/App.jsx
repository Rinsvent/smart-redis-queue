import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  Alert,
  AppBar,
  Box,
  Button,
  Checkbox,
  CircularProgress,
  CssBaseline,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  FormControl,
  FormControlLabel,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
  Snackbar,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  ThemeProvider,
  Toolbar,
  Typography,
  createTheme,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DarkModeOutlinedIcon from '@mui/icons-material/DarkModeOutlined'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline'
import LightModeOutlinedIcon from '@mui/icons-material/LightModeOutlined'
import RefreshIcon from '@mui/icons-material/Refresh'
import { api } from './api'

const FONT = '"DM Sans", "Segoe UI", sans-serif'

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

export default function App() {
  const [mode, setMode] = useState(() => localStorage.getItem('srq-theme') || 'light')
  const theme = useMemo(() => buildTheme(mode), [mode])

  const [connections, setConnections] = useState([])
  const [activeId, setActiveId] = useState('')
  const [queues, setQueues] = useState([])
  const [selectedQueue, setSelectedQueue] = useState('')
  const [loading, setLoading] = useState(false)
  const [snack, setSnack] = useState({ open: false, msg: '', severity: 'info' })

  const [addOpen, setAddOpen] = useState(false)
  const [form, setForm] = useState({ name: '', addr: 'localhost:6379', password: '', db: 0 })
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

  const [confirm, setConfirm] = useState(null)
  const pollRef = useRef(null)

  const notify = (msg, severity = 'info') => setSnack({ open: true, msg, severity })

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

  useEffect(() => {
    loadConnections()
  }, [loadConnections])

  useEffect(() => {
    loadQueues()
    if (pollRef.current) clearInterval(pollRef.current)
    if (!activeId) return undefined
    pollRef.current = setInterval(loadQueues, 2000)
    return () => {
      if (pollRef.current) clearInterval(pollRef.current)
    }
  }, [activeId, loadQueues])

  useEffect(() => {
    localStorage.setItem('srq-theme', mode)
  }, [mode])

  const selected = queues.find((q) => q.name === selectedQueue) || null

  const onSelectConn = async (id) => {
    try {
      await api.selectConnection(id)
      setActiveId(id)
      setSelectedQueue('')
      notify('Подключение выбрано')
    } catch (e) {
      notify(String(e), 'error')
    }
  }

  const onAdd = async () => {
    setTesting(true)
    try {
      await api.testConnection(form.addr, form.password, Number(form.db) || 0)
      const c = await api.addConnection(form.name, form.addr, form.password, Number(form.db) || 0)
      setAddOpen(false)
      setForm({ name: '', addr: 'localhost:6379', password: '', db: 0 })
      await loadConnections()
      await onSelectConn(c.id)
      notify('Подключение сохранено', 'success')
    } catch (e) {
      notify(String(e), 'error')
    } finally {
      setTesting(false)
    }
  }

  const onDeleteConn = () => {
    if (!activeId) return
    setConfirm({
      title: 'Удалить подключение?',
      body: 'Профиль будет удалён из списка. Данные в Redis не затрагиваются.',
      action: async () => {
        try {
          await api.removeConnection(activeId)
          setSelectedQueue('')
          await loadConnections()
          notify('Удалено')
        } catch (e) {
          notify(String(e), 'error')
        }
      },
    })
  }

  const runPurge = (partition) => {
    if (!selected) return
    setConfirm({
      title: partition ? `Purge partition ${partition}?` : `Purge queue ${selected.name}?`,
      body: 'Все pending-задачи будут удалены.',
      action: async () => {
        try {
          const n = await api.purge(selected.name, partition || '')
          notify(`Удалено задач: ${n}`, 'success')
          loadQueues()
        } catch (e) {
          notify(String(e), 'error')
        }
      },
    })
  }

  const runRetry = () => {
    if (!selected) return
    setConfirm({
      title: `Retry ${selected.name}?`,
      body: 'In-progress задачи вернутся в pending.',
      action: async () => {
        try {
          const n = await api.retry(selected.name)
          notify(`Возвращено: ${n}`, 'success')
          loadQueues()
        } catch (e) {
          notify(String(e), 'error')
        }
      },
    })
  }

  const loadPayload = async () => {
    if (!selected || !payloadId.trim()) return
    try {
      const r = await api.getTaskPayload(selected.name, payloadId.trim())
      setPayloadResult(r)
    } catch (e) {
      setPayloadResult(null)
      notify(String(e), 'error')
    }
  }

  const runTag = async () => {
    if (!selected || !tag.trim()) return
    try {
      if (tagMode === 'count') {
        const n = await api.countByTag(selected.name, tag.trim())
        setTagResult({ count: n })
      } else {
        const r = await api.removeByTag(selected.name, tag.trim(), Number(tagLimit) || 0, tagPreview)
        setTagResult(r)
        loadQueues()
      }
    } catch (e) {
      notify(String(e), 'error')
    }
  }

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
        <AppBar position="sticky" color="transparent" elevation={0}>
          <Toolbar sx={{ gap: 2, flexWrap: 'wrap' }}>
            <Typography variant="h6" sx={{ mr: 1, color: 'text.primary' }}>
              Smart Redis Queue
            </Typography>
            {connections.length > 0 && (
              <>
                <FormControl size="small" sx={{ minWidth: 200 }}>
                  <InputLabel>База</InputLabel>
                  <Select
                    label="База"
                    value={activeId || ''}
                    onChange={(e) => onSelectConn(e.target.value)}
                  >
                    {connections.map((c) => (
                      <MenuItem key={c.id} value={c.id}>
                        {c.name} ({c.addr}/{c.db})
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
                <IconButton onClick={() => setAddOpen(true)} title="Добавить">
                  <AddIcon />
                </IconButton>
                <IconButton onClick={onDeleteConn} disabled={!activeId} title="Удалить">
                  <DeleteOutlineIcon />
                </IconButton>
                <IconButton onClick={loadQueues} disabled={!activeId || loading} title="Обновить">
                  <RefreshIcon />
                </IconButton>
              </>
            )}
            <Box sx={{ flexGrow: 1 }} />
            <IconButton
              onClick={() => setMode((m) => (m === 'light' ? 'dark' : 'light'))}
              title="Тема"
            >
              {mode === 'light' ? <DarkModeOutlinedIcon /> : <LightModeOutlinedIcon />}
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
              Добавьте профиль Redis — после этого появятся очереди и действия администратора.
            </Typography>
            <Button
              variant="contained"
              size="large"
              startIcon={<AddIcon />}
              onClick={() => setAddOpen(true)}
              sx={{ mt: 1, px: 4, py: 1.5, fontSize: '1.05rem' }}
            >
              Добавить подключение
            </Button>
          </Box>
        ) : (
          <Box
            sx={{
              flex: 1,
              display: 'grid',
              gridTemplateColumns: { xs: '1fr', md: '340px 1fr' },
              gap: 0,
              minHeight: 0,
            }}
          >
            <Box
              sx={{
                borderRight: 1,
                borderColor: 'divider',
                p: 2,
                overflow: 'auto',
                bgcolor: 'background.paper',
              }}
            >
              <Stack direction="row" alignItems="center" justifyContent="space-between" mb={1}>
                <Typography variant="subtitle1">Очереди</Typography>
                {loading && <CircularProgress size={18} />}
              </Stack>
              {queues.length === 0 ? (
                <Typography variant="body2" color="text.secondary">
                  Очереди не найдены (SCAN queue:*:partitions)
                </Typography>
              ) : (
                <Stack spacing={0.5}>
                  {queues.map((q) => (
                    <Box
                      key={q.name}
                      onClick={() => setSelectedQueue(q.name)}
                      sx={{
                        p: 1.25,
                        borderRadius: 2,
                        cursor: 'pointer',
                        border: 1,
                        borderColor: selectedQueue === q.name ? 'primary.main' : 'divider',
                        bgcolor: selectedQueue === q.name ? 'action.selected' : 'transparent',
                        '&:hover': { bgcolor: 'action.hover' },
                      }}
                    >
                      <Typography fontWeight={600}>{q.name}</Typography>
                      <Typography variant="caption" color="text.secondary" display="block">
                        pending {q.pending} · in-progress {q.inProgress} · cons {q.consumers}
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        {rateLabel(q.rate)}
                      </Typography>
                    </Box>
                  ))}
                </Stack>
              )}
            </Box>

            <Box sx={{ p: 3, overflow: 'auto' }}>
              {!selected ? (
                <Typography color="text.secondary">Выберите очередь слева</Typography>
              ) : (
                <Stack spacing={2.5}>
                  <Box>
                    <Typography variant="h5">{selected.name}</Typography>
                    <Typography color="text.secondary">{rateLabel(selected.rate)}</Typography>
                  </Box>
                  <Stack direction="row" spacing={3} flexWrap="wrap">
                    <Stat label="Pending" value={selected.pending} />
                    <Stat label="In progress" value={selected.inProgress} />
                    <Stat label="Consumers" value={selected.consumers} />
                    <Stat label="Partitions" value={(selected.partitions || []).length} />
                  </Stack>
                  <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                    <Button variant="outlined" onClick={loadQueues}>
                      Refresh
                    </Button>
                    <Button variant="outlined" color="warning" onClick={() => runPurge('')}>
                      Purge queue
                    </Button>
                    <Button variant="outlined" onClick={runRetry}>
                      Retry
                    </Button>
                    <Button
                      variant="outlined"
                      onClick={() => {
                        setPayloadResult(null)
                        setPayloadId('')
                        setPayloadOpen(true)
                      }}
                    >
                      Payload by ID
                    </Button>
                    <Button
                      variant="outlined"
                      onClick={() => {
                        setTagMode('count')
                        setTagResult(null)
                        setTag('')
                        setTagOpen(true)
                      }}
                    >
                      Count by tag
                    </Button>
                    <Button
                      variant="outlined"
                      color="warning"
                      onClick={() => {
                        setTagMode('remove')
                        setTagResult(null)
                        setTag('')
                        setTagOpen(true)
                      }}
                    >
                      Remove by tag
                    </Button>
                  </Stack>
                  <Divider />
                  <Typography variant="subtitle1">Partitions</Typography>
                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell>ID</TableCell>
                        <TableCell align="right">Pending</TableCell>
                        <TableCell>Flags</TableCell>
                        <TableCell align="right">Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {(selected.partitions || []).map((p) => (
                        <TableRow key={p.id}>
                          <TableCell>{p.id}</TableCell>
                          <TableCell align="right">{p.pending}</TableCell>
                          <TableCell>
                            {[p.locked && 'locked', p.blocked && 'blocked'].filter(Boolean).join(', ') ||
                              '—'}
                          </TableCell>
                          <TableCell align="right">
                            <Button size="small" color="warning" onClick={() => runPurge(p.id)}>
                              Purge
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </Stack>
              )}
            </Box>
          </Box>
        )}
      </Box>

      <Dialog open={addOpen} onClose={() => setAddOpen(false)} fullWidth maxWidth="sm">
        <DialogTitle>Новое подключение Redis</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            <TextField
              label="Имя"
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
            <TextField
              label="DB"
              type="number"
              value={form.db}
              onChange={(e) => setForm({ ...form, db: e.target.value })}
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setAddOpen(false)}>Отмена</Button>
          <Button variant="contained" onClick={onAdd} disabled={testing || !form.name || !form.addr}>
            {testing ? 'Проверка…' : 'Сохранить'}
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
          <Button onClick={() => setPayloadOpen(false)}>Закрыть</Button>
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
          <Button onClick={() => setTagOpen(false)}>Закрыть</Button>
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
          <Button onClick={() => setConfirm(null)}>Отмена</Button>
          <Button
            color="warning"
            variant="contained"
            onClick={async () => {
              const a = confirm?.action
              setConfirm(null)
              if (a) await a()
            }}
          >
            Подтвердить
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
    <Box>
      <Typography variant="caption" color="text.secondary">
        {label}
      </Typography>
      <Typography variant="h5">{value ?? 0}</Typography>
    </Box>
  )
}
