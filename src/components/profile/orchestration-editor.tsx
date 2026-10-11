import {
  AddRounded,
  DeleteOutlineRounded,
  RefreshRounded,
} from '@mui/icons-material'
import {
  Box,
  Button,
  Chip,
  Divider,
  FormControl,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
  Stack,
  TextField,
  Typography,
} from '@mui/material'
import { useLockFn } from 'ahooks'
import { load } from 'js-yaml'
import { useEffect, useState } from 'react'

import { BaseDialog } from '@/components/base'
import {
  enhanceProfiles,
  fetchOrchestrationSource,
  getOrchestrationState,
  readProfileFile,
  saveOrchestrationState,
} from '@/services/cmds'
import { mutate } from '@/services/mutate'
import { showNotice } from '@/services/notice-service'
import type {
  GroupMode,
  OrchestrationNodeRef,
  OrchestrationNodeSummary,
  OrchestrationSourceFetchResult,
  OrchestrationSource,
  OrchestrationState,
} from '@/types/orchestration'

interface Props {
  profileUid: string
  open: boolean
  onClose: () => void
}
interface GroupInfo {
  name: string
  type: string
}

type CustomGroupDefinition = Record<string, unknown>

const emptyState = (): OrchestrationState => ({
  sources: [],
  active_mode: 'upstream',
  default_mode: 'upstream',
  groups: {},
})
const newSourceId = () => 'source-' + crypto.randomUUID()

export const OrchestrationEditor = ({ profileUid, open, onClose }: Props) => {
  const [state, setState] = useState<OrchestrationState>(emptyState)
  const [groups, setGroups] = useState<GroupInfo[]>([])
  const [nodes, setNodes] = useState<
    Record<string, OrchestrationNodeSummary[]>
  >({})
  const [selectedSource, setSelectedSource] = useState('')
  const [sourceName, setSourceName] = useState('External subscription')
  const [sourceUrl, setSourceUrl] = useState('')
  const [customDrafts, setCustomDrafts] = useState<Record<string, string>>({})
  const [loading, setLoading] = useState(false)
  const [dirty, setDirty] = useState(false)
  const [sourceStatus, setSourceStatus] = useState<
    Record<string, OrchestrationSourceFetchResult>
  >({})
  const [sourceErrors, setSourceErrors] = useState<Record<string, string>>({})

  const refreshSource = async (source: OrchestrationSource) => {
    setLoading(true)
    try {
      const result = await fetchOrchestrationSource(source, profileUid)
      setNodes((current) => ({ ...current, [source.source_id]: result.nodes }))
      setSourceStatus((current) => ({ ...current, [source.source_id]: result }))
      setSourceErrors((current) => {
        const next = { ...current }
        delete next[source.source_id]
        return next
      })
    } catch (error) {
      setNodes((current) => ({ ...current, [source.source_id]: [] }))
      setSourceErrors((current) => ({
        ...current,
        [source.source_id]:
          error instanceof Error ? error.message : String(error),
      }))
      showNotice.error(error)
    } finally {
      setLoading(false)
    }
  }

  const loadEditor = useLockFn(async () => {
    setLoading(true)
    try {
      const [saved, raw] = await Promise.all([
        getOrchestrationState(profileUid),
        readProfileFile(profileUid),
      ])
      const document = load(raw) as {
        'proxy-groups'?: { name?: string; type?: string }[]
      } | null
      setGroups(
        (document?.['proxy-groups'] ?? [])
          .filter((group) => group.name)
          .map((group) => ({
            name: group.name as string,
            type: group.type ?? 'select',
          })),
      )
      setState({ ...emptyState(), ...saved, groups: saved.groups ?? {} })
      setCustomDrafts(
        Object.fromEntries(
          Object.entries(saved.groups ?? {})
            .filter(([, binding]) => binding.custom)
            .map(([name, binding]) => [
              name,
              JSON.stringify(binding.custom, null, 2),
            ]),
        ),
      )
      setSelectedSource(saved.sources[0]?.source_id ?? '')
      const fetched = await Promise.all(
        saved.sources
          .filter((source) => source.enabled)
          .map(async (source) => {
            try {
              const result = await fetchOrchestrationSource(source, profileUid)
              setSourceStatus((current) => ({
                ...current,
                [source.source_id]: result,
              }))
              return [source.source_id, result.nodes] as const
            } catch {
              return [source.source_id, []] as const
            }
          }),
      )
      setNodes(Object.fromEntries(fetched))
      setDirty(false)
    } catch (error) {
      showNotice.error(error)
    } finally {
      setLoading(false)
    }
  })

  useEffect(() => {
    if (open) void loadEditor()
  }, [loadEditor, open])

  const addSource = () => {
    const url = sourceUrl.trim()
    if (!/^https?:\/\//i.test(url)) {
      showNotice.error('The source URL must start with http:// or https://')
      return
    }
    const source: OrchestrationSource = {
      source_id: newSourceId(),
      name: sourceName.trim() || 'External subscription',
      url,
      enabled: true,
    }
    setState((current) => ({
      ...current,
      sources: [...current.sources, source],
    }))
    setSelectedSource(source.source_id)
    setSourceUrl('')
    setDirty(true)
    void refreshSource(source)
  }

  const removeSource = (sourceId: string) => {
    setState((current) => ({
      ...current,
      sources: current.sources.filter(
        (source) => source.source_id !== sourceId,
      ),
      groups: Object.fromEntries(
        Object.entries(current.groups).map(([name, binding]) => [
          name,
          {
            ...binding,
            nodes: binding.nodes.filter((node) => node.source_id !== sourceId),
          },
        ]),
      ),
    }))
    setDirty(true)
  }

  const addNode = (groupName: string, node: OrchestrationNodeSummary) => {
    const source = state.sources.find(
      (item) => item.source_id === node.source_id,
    )
    const ref: OrchestrationNodeRef = {
      source_id: node.source_id,
      node_id: node.node_id,
      name: '[' + (source?.name ?? node.source_id) + '] ' + node.name,
    }
    setState((current) => {
      const previous = current.groups[groupName] ?? { nodes: [] }
      if (
        previous.nodes.some(
          (item) =>
            item.source_id === ref.source_id && item.node_id === ref.node_id,
        )
      )
        return current
      return {
        ...current,
        groups: {
          ...current.groups,
          [groupName]: {
            ...previous,
            mode:
              previous.mode && previous.mode !== 'upstream'
                ? previous.mode
                : 'hybrid',
            nodes: [...previous.nodes, ref],
          },
        },
      }
    })
    setDirty(true)
  }

  const removeNode = (groupName: string, node: OrchestrationNodeRef) => {
    setState((current) => ({
      ...current,
      groups: {
        ...current.groups,
        [groupName]: {
          ...(current.groups[groupName] ?? { nodes: [] }),
          nodes: (current.groups[groupName]?.nodes ?? []).filter(
            (item) =>
              item.source_id !== node.source_id ||
              item.node_id !== node.node_id,
          ),
        },
      },
    }))
    setDirty(true)
  }

  const setGroupMode = (groupName: string, mode: GroupMode) => {
    setState((current) => ({
      ...current,
      groups: {
        ...current.groups,
        [groupName]: {
          ...(current.groups[groupName] ?? { nodes: [] }),
          mode,
          ...(mode === 'custom' && !current.groups[groupName]?.custom
            ? {
                custom: {
                  name: groupName,
                  type: 'select',
                  proxies: [],
                },
              }
            : {}),
        },
      },
    }))
    if (mode === 'custom' && !customDrafts[groupName]) {
      setCustomDrafts((current) => ({
        ...current,
        [groupName]: JSON.stringify(
          { name: groupName, type: 'select', proxies: [] },
          null,
          2,
        ),
      }))
    }
    setDirty(true)
  }

  const updateCustomGroup = (groupName: string, text: string) => {
    setCustomDrafts((current) => ({ ...current, [groupName]: text }))
    try {
      const custom = JSON.parse(text) as CustomGroupDefinition
      if (!custom || Array.isArray(custom) || typeof custom !== 'object') return
      setState((current) => ({
        ...current,
        groups: {
          ...current.groups,
          [groupName]: {
            ...(current.groups[groupName] ?? { nodes: [] }),
            custom,
          },
        },
      }))
    } catch {
      // Keep the draft visible and let the field show the invalid JSON state.
    }
    setDirty(true)
  }

  const save = useLockFn(async () => {
    for (const [groupName, binding] of Object.entries(state.groups)) {
      if (binding.mode !== 'custom') continue
      const draft = customDrafts[groupName]
      try {
        const custom = JSON.parse(draft ?? JSON.stringify(binding.custom ?? {}))
        if (!custom || Array.isArray(custom) || typeof custom !== 'object') {
          throw new Error('custom group must be a JSON object')
        }
      } catch {
        showNotice.error('Custom group ' + groupName + ' contains invalid JSON')
        return
      }
    }
    setLoading(true)
    try {
      await mutate(() => saveOrchestrationState(profileUid, state), {
        id: 'save-orchestration:' + profileUid,
        errorNotice: false,
      })
      await mutate(() => enhanceProfiles(), {
        id: 'enhance-profiles-after-orchestration',
        errorNotice: false,
      })
      setDirty(false)
      showNotice.success('Orchestration saved and profile regenerated')
      onClose()
    } catch (error) {
      showNotice.error(error)
    } finally {
      setLoading(false)
    }
  })

  const selectedNodes = nodes[selectedSource] ?? []

  return (
    <BaseDialog
      open={open}
      title="Subscription orchestration"
      okBtn="Save and apply"
      cancelBtn="Close"
      loading={loading}
      disableOk={!dirty}
      contentSx={{ width: { xs: 360, sm: 720 }, maxWidth: '90vw' }}
      onCancel={onClose}
      onClose={onClose}
      onOk={save}
    >
      <Stack spacing={2}>
        <FormControl fullWidth size="small">
          <InputLabel>Global mode</InputLabel>
          <Select
            label="Global mode"
            value={state.active_mode}
            onChange={(event) => {
              setState((current) => ({
                ...current,
                active_mode: event.target.value as GroupMode,
              }))
              setDirty(true)
            }}
          >
            <MenuItem value="upstream">Use profile groups</MenuItem>
            <MenuItem value="hybrid">Append external nodes</MenuItem>
            <MenuItem value="custom">Use custom group definitions</MenuItem>
          </Select>
        </FormControl>
        <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1}>
          <TextField
            size="small"
            fullWidth
            label="Source name"
            value={sourceName}
            onChange={(event) => setSourceName(event.target.value)}
          />
          <TextField
            size="small"
            fullWidth
            label="Subscription URL"
            value={sourceUrl}
            onChange={(event) => setSourceUrl(event.target.value)}
          />
          <Button
            variant="outlined"
            startIcon={<AddRounded />}
            onClick={addSource}
          >
            Add
          </Button>
        </Stack>
        {state.sources.map((source) => (
          <Box
            key={source.source_id}
            sx={{ display: 'flex', alignItems: 'center', gap: 1 }}
          >
            <Chip
              label={(() => {
                const result = sourceStatus[source.source_id]
                const error = sourceErrors[source.source_id]
                const status = error
                  ? 'failed · no usable cache'
                  : result?.used_cache
                    ? 'cached · last success ' +
                    new Date(result.fetched_at * 1000).toLocaleString()
                    : result
                      ? 'live · ' +
                        new Date(result.fetched_at * 1000).toLocaleString()
                      : 'not fetched'
                return source.name + ' · ' + status
              })()}
              color={sourceErrors[source.source_id] ? 'error' : undefined}
              sx={{ maxWidth: 'calc(100% - 80px)' }}
            />
            <IconButton
              size="small"
              onClick={() => void refreshSource(source)}
              title="Refresh source"
            >
              <RefreshRounded />
            </IconButton>
            <IconButton
              size="small"
              onClick={() => removeSource(source.source_id)}
              title="Remove source"
            >
              <DeleteOutlineRounded />
            </IconButton>
          </Box>
        ))}
        <Divider />
        <FormControl fullWidth size="small">
          <InputLabel>Node source</InputLabel>
          <Select
            label="Node source"
            value={selectedSource}
            onChange={(event) => setSelectedSource(event.target.value)}
          >
            {state.sources.map((source) => (
              <MenuItem key={source.source_id} value={source.source_id}>
                {source.name}
              </MenuItem>
            ))}
          </Select>
        </FormControl>
        <Box
          sx={{
            display: 'grid',
            gridTemplateColumns: 'minmax(180px, 1fr) minmax(220px, 1.3fr)',
            gap: 2,
          }}
        >
          <Box>
            <Typography variant="subtitle2" gutterBottom>
              External nodes
            </Typography>
            <Stack spacing={0.5} sx={{ maxHeight: 260, overflow: 'auto' }}>
              {selectedNodes.map((node) => (
                <Chip
                  key={node.node_id}
                  draggable
                  onDragStart={(event) =>
                    event.dataTransfer.setData(
                      'application/orchestration-node',
                      JSON.stringify(node),
                    )
                  }
                  label={node.name + ' · ' + node.proxy_type}
                  variant="outlined"
                  sx={{ justifyContent: 'flex-start', cursor: 'grab' }}
                />
              ))}
            </Stack>
          </Box>
          <Box>
            <Typography variant="subtitle2" gutterBottom>
              Profile groups
            </Typography>
            <Stack spacing={1} sx={{ maxHeight: 320, overflow: 'auto' }}>
              {groups.map((group) => {
                const binding = state.groups[group.name] ?? { nodes: [] }
                return (
                  <Box
                    key={group.name}
                    onDragOver={(event) => event.preventDefault()}
                    onDrop={(event) => {
                      event.preventDefault()
                      const raw = event.dataTransfer.getData(
                        'application/orchestration-node',
                      )
                      if (raw)
                        addNode(
                          group.name,
                          JSON.parse(raw) as OrchestrationNodeSummary,
                        )
                    }}
                    sx={{
                      border: '1px solid',
                      borderColor: 'divider',
                      borderRadius: 1,
                      p: 1,
                    }}
                  >
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <Typography sx={{ flex: 1 }} variant="body2">
                        {group.name}{' '}
                        <Typography component="span" variant="caption">
                          ({group.type})
                        </Typography>
                      </Typography>
                      <Select
                        size="small"
                        value={binding.mode ?? 'upstream'}
                        onChange={(event) =>
                          setGroupMode(
                            group.name,
                            event.target.value as GroupMode,
                          )
                        }
                      >
                        <MenuItem value="upstream">Upstream</MenuItem>
                        <MenuItem value="hybrid">Hybrid</MenuItem>
                        <MenuItem value="custom">Custom</MenuItem>
                      </Select>
                    </Box>
                    {binding.mode === 'custom' && (
                      <TextField
                        fullWidth
                        multiline
                        minRows={4}
                        size="small"
                        label="Custom group definition (JSON)"
                        value={
                          customDrafts[group.name] ??
                          JSON.stringify(
                            binding.custom ?? {
                              name: group.name,
                              type: 'select',
                              proxies: [],
                            },
                            null,
                            2,
                          )
                        }
                        error={(() => {
                          try {
                            const value = JSON.parse(
                              customDrafts[group.name] ?? '{}',
                            )
                            return !value || Array.isArray(value)
                          } catch {
                            return true
                          }
                        })()}
                        helperText="Edit any Mihomo proxy-group fields. Use valid JSON."
                        onChange={(event) =>
                          updateCustomGroup(group.name, event.target.value)
                        }
                        sx={{ mt: 1 }}
                      />
                    )}
                    <Box
                      sx={{
                        display: 'flex',
                        flexWrap: 'wrap',
                        gap: 0.5,
                        mt: 1,
                        minHeight: 28,
                      }}
                    >
                      {binding.nodes.map((node) => (
                        <Chip
                          key={node.source_id + ':' + node.node_id}
                          size="small"
                          label={node.name}
                          onDelete={() => removeNode(group.name, node)}
                        />
                      ))}
                    </Box>
                  </Box>
                )
              })}
            </Stack>
          </Box>
        </Box>
      </Stack>
    </BaseDialog>
  )
}
