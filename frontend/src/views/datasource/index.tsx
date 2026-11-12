import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/datasource'
const columns = [
  '配置编号',
  '连接类型',
  '接入地址',
  '认证方式',
  '连接池大小',
  '字符编码',
  '查询超时秒数',
  '重试上限'
]

type Step = { 阶段: string; 状态: string; 说明: string; 耗时毫秒: number; 尝试序号: number }
type Diagnosis = {
  诊断编号: string
  诊断结论: string
  失败阶段: string | null
  步骤: Step[]
  尝试次数: number
  预算秒数: number
  总耗时毫秒: number
  诊断时间: string
  是否过期: boolean
  结论说明: string
}
type Row = Record<string, string | number | boolean | null> & {
  id: number
  status: string
  最近诊断?: Diagnosis | null
}
type ActionResponse = {
  ok: boolean
  message: string
  entry?: Row
  detail?: { waiter_id?: string; 诊断?: Diagnosis }
}

const phaseTone = (status: string) => {
  if (status === '成功') return 'phase-ok'
  if (status === '失败') return 'phase-fail'
  if (status === '取消') return 'phase-cancel'
  return 'phase-skip'
}

export default function DatasourceView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [busyId, setBusyId] = useState<number | null>(null)
  const [waiter, setWaiter] = useState<{ rowId: number; waiterId: string } | null>(null)
  const [openDiag, setOpenDiag] = useState<Diagnosis | null>(null)
  const [editing, setEditing] = useState<Row | null>(null)
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) throw new Error('数据源配置列表读取失败')
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '数据源连接列表读取失败')
    }
  }, [filters])

  useEffect(() => {
    void reload()
  }, [reload])

  async function diagnose(row: Row) {
    setBusyId(row.id)
    setErrorMessage('')
    setNotice('')
    const waiterId = `web-${row.id}-${Date.now()}`
    setWaiter({ rowId: row.id, waiterId })
    try {
      const response = await request(`${ENDPOINT}/${row.id}/diagnose`, {
        method: 'POST',
        body: JSON.stringify({ values: { waiter_id: waiterId } }),
      })
      const body = (await response.json()) as ActionResponse
      if (body.detail?.诊断) setOpenDiag(body.detail.诊断)
      if (body.ok) {
        setNotice(`连接诊断通过：${body.message}`)
      } else {
        setErrorMessage(body.message)
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '连接诊断请求失败')
    } finally {
      setBusyId(null)
      setWaiter(null)
    }
  }

  async function cancelWaiting() {
    if (!waiter) return
    try {
      const response = await request(`${ENDPOINT}/${waiter.rowId}/diagnose/cancel`, {
        method: 'POST',
        body: JSON.stringify({ values: { waiter_id: waiter.waiterId } }),
      })
      const body = (await response.json()) as ActionResponse
      setNotice(body.message)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '取消诊断失败')
    }
  }

  async function runSimpleAction(action: string, row: Row) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ values: { action } }),
      })
      const body = (await response.json()) as ActionResponse
      if (!body.ok) setErrorMessage(body.message)
      else setNotice(body.message)
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '数据源操作失败')
    }
  }

  async function openHistory(row: Row) {
    try {
      const response = await request(`${ENDPOINT}/${row.id}/diagnoses`)
      const body = await response.json()
      const latest = (body.items ?? [])[0] as Diagnosis | undefined
      setOpenDiag(latest ?? null)
      if (!latest) setNotice('该配置还没有诊断记录')
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '诊断历史读取失败')
    }
  }

  const connected = rows.filter((row) => row.status === '已连接').length
  const stats = [
    { label: '数据源配置总数', value: total },
    { label: '已连接数据源', value: connected },
    { label: '待校验数据源', value: rows.filter((row) => row.status === '待校验').length },
  ]

  return (
    <section className="page" data-module="datasource">
      <header className="page-head">
        <div>
          <h2>数据源连接管理</h2>
          <p className="page-desc">
            对当前配置执行只读连接诊断，分阶段核对地址格式、握手、认证与读取探针；诊断结论是 ETL 输入源准入的唯一依据。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出数据源连接清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        {stats.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>最近诊断</th>
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
              <td>
                {row.最近诊断 ? (
                  <button className="link" type="button" onClick={() => setOpenDiag(row.最近诊断 ?? null)}>
                    <span className={row.最近诊断.诊断结论 === '成功' ? 'phase-ok' : 'phase-fail'}>
                      {row.最近诊断.是否过期 ? `${row.最近诊断.诊断结论}（已过期）` : row.最近诊断.诊断结论}
                    </span>
                  </button>
                ) : (
                  <span className="phase-skip">未诊断</span>
                )}
              </td>
              <td className="row-actions">
                <button
                  className="link"
                  type="button"
                  disabled={busyId === row.id}
                  onClick={() => void diagnose(row)}
                >
                  {busyId === row.id ? '诊断进行中…' : '连接诊断'}
                </button>
                {waiter?.rowId === row.id && (
                  <button className="link phase-cancel" type="button" onClick={() => void cancelWaiting()}>
                    取消等待
                  </button>
                )}
                <button className="link" type="button" onClick={() => void runSimpleAction('提交校验', row)}>提交校验</button>
                <button className="link" type="button" onClick={() => void runSimpleAction('断开连接', row)}>断开连接</button>
                <button className="link" type="button" onClick={() => void openHistory(row)}>诊断历史</button>
                <button className="link" type="button" onClick={() => setEditing(row)}>修改配置</button>
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 2}>暂无数据源连接数据</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条数据源连接记录</span>
        {notice && <span className="phase-ok">{notice}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>

      {openDiag && <DiagnosisModal diagnosis={openDiag} onClose={() => setOpenDiag(null)} />}
      {editing && (
        <EditConfigModal
          row={editing}
          onClose={() => setEditing(null)}
          onSaved={async () => {
            setEditing(null)
            setNotice('配置已更新；认证方式、地址或超时/重试变更后，旧诊断已失效，需要重新诊断')
            await reload()
          }}
        />
      )}
    </section>
  )
}

function DiagnosisModal({ diagnosis, onClose }: { diagnosis: Diagnosis; onClose: () => void }) {
  return (
    <div className="modal-mask" onClick={onClose}>
      <div className="modal-card" onClick={(event) => event.stopPropagation()}>
        <h3>连接诊断详情</h3>
        <p className="page-desc">
          诊断编号 {diagnosis.诊断编号} · {diagnosis.诊断时间} · 预算 {diagnosis.预算秒数} 秒 ·
          尝试 {diagnosis.尝试次数} 次 · 总耗时 {diagnosis.总耗时毫秒} 毫秒
        </p>
        <p>
          总体结论：
          <strong className={diagnosis.诊断结论 === '成功' ? 'phase-ok' : 'phase-fail'}>
            {diagnosis.诊断结论}
          </strong>
          {diagnosis.失败阶段 && <> · 失败阶段：<strong className="phase-fail">{diagnosis.失败阶段}</strong></>}
          {diagnosis.是否过期 && <strong className="phase-cancel"> · 该诊断已过期，不能作为准入依据</strong>}
        </p>
        <table className="data-table">
          <thead>
            <tr><th>阶段</th><th>状态</th><th>说明</th><th>耗时毫秒</th><th>尝试序号</th></tr>
          </thead>
          <tbody>
            {diagnosis.步骤.map((step, idx) => (
              <tr key={`${step.阶段}-${idx}`}>
                <td>{step.阶段}</td>
                <td className={phaseTone(step.状态)}>{step.状态}</td>
                <td>{step.说明}</td>
                <td>{step.耗时毫秒}</td>
                <td>{step.尝试序号 || '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <p className="page-desc">{diagnosis.结论说明}</p>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={onClose}>关闭</button>
        </div>
      </div>
    </div>
  )
}

function EditConfigModal({
  row,
  onClose,
  onSaved,
}: {
  row: Row
  onClose: () => void
  onSaved: () => void
}) {
  const [form, setForm] = useState<Record<string, string>>({
    接入地址: String(row.接入地址 ?? ''),
    认证方式: String(row.认证方式 ?? '无认证'),
    连接池大小: String(row.连接池大小 ?? '2'),
    字符编码: String(row.字符编码 ?? 'UTF-8'),
    查询超时秒数: String(row.查询超时秒数 ?? '5'),
    重试上限: String(row.重试上限 ?? '1'),
  })
  const [secret, setSecret] = useState('')
  const [error, setError] = useState('')

  async function save() {
    setError('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}`, {
        method: 'PATCH',
        body: JSON.stringify({ values: { ...form, 认证凭据: secret } }),
      })
      const body = (await response.json()) as ActionResponse
      if (!body.ok) {
        setError(body.message)
        return
      }
      onSaved()
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : '配置更新失败')
    }
  }

  return (
    <div className="modal-mask" onClick={onClose}>
      <div className="modal-card" onClick={(event) => event.stopPropagation()}>
        <h3>修改数据源配置</h3>
        <p className="page-desc">
          认证方式、接入地址、查询超时秒数或重试上限变更后，历史诊断立即过期；认证凭据仅保存于服务端，不在任何页面回显。
        </p>
        {Object.entries(form).map(([key, value]) => (
          <label key={key} className="filter-item modal-field">
            <span>{key}</span>
            <input value={value} onChange={(event) => setForm((prev) => ({ ...prev, [key]: event.target.value }))} />
          </label>
        ))}
        <label className="filter-item modal-field">
          <span>认证凭据{row.已登记认证凭据 ? '（已登记，留空保持不变）' : ''}</span>
          <input
            type="password"
            value={secret}
            placeholder="输入新凭据；清空并保存可移除已登记凭据"
            onChange={(event) => setSecret(event.target.value)}
          />
        </label>
        {error && <p className="error-text">{error}</p>}
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => void save()}>保存并使旧诊断失效</button>
          <button className="btn ghost" type="button" onClick={onClose}>取消</button>
        </div>
      </div>
    </div>
  )
}
