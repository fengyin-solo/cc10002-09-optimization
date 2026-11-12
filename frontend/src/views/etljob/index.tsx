import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/etljob'
const columns = [
  '任务编号',
  '输入源',
  '输出目标',
  '转换脚本',
  '依赖任务',
  '调度表达式',
  '数据分区',
  '失败重试次数'
]

type InputCheck = {
  输入源文字: string
  绑定: boolean
  数据源配置ID: number | null
  配置编号?: string
  判定: string
  采用诊断编号: string | null
  结论: string
}
type Admission = {
  准入编号: string
  检查时间: string
  允许启动: boolean
  输入源检查: InputCheck[]
  说明: string
}
type Row = Record<string, string | number | null> & {
  id: number
  status: string
  最近准入?: Admission | null
}
type ActionResponse = {
  ok: boolean
  message: string
  entry?: Row
  detail?: { 准入编号?: string; 输入源检查?: InputCheck[]; 最近准入?: Admission }
}

const verdictTone = (verdict: string) => {
  if (verdict === '准入通过') return 'phase-ok'
  if (verdict === '拒绝准入') return 'phase-fail'
  return 'phase-cancel'
}

export default function EtljobView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [openAdmission, setOpenAdmission] = useState<Admission | null>(null)
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) throw new Error('加工任务列表读取失败')
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : 'ETL任务列表读取失败')
    }
  }, [filters])

  useEffect(() => {
    void reload()
  }, [reload])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ values: { action } }),
      })
      const body = (await response.json()) as ActionResponse
      if (!body.ok) {
        setErrorMessage(body.message)
        if (body.detail?.输入源检查) {
          setOpenAdmission({
            准入编号: body.detail.准入编号 ?? '',
            检查时间: '',
            允许启动: false,
            输入源检查: body.detail.输入源检查,
            说明: body.message,
          })
        }
      } else {
        setNotice(body.message)
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : 'ETL任务操作失败')
    }
  }

  async function checkInputs(row: Row) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/input-check`, { method: 'POST' })
      if (!response.ok) throw new Error('输入源准入核对失败')
      const report = (await response.json()) as Admission
      setOpenAdmission(report)
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '输入源准入核对失败')
    }
  }

  const stats = [
    { label: '加工任务总数', value: total },
    { label: '运行中任务', value: rows.filter((row) => row.status === '运行中').length },
    { label: '已完成任务', value: rows.filter((row) => row.status === '已完成').length },
  ]

  return (
    <section className="page" data-module="etljob">
      <header className="page-head">
        <div>
          <h2>ETL任务管理</h2>
          <p className="page-desc">
            启动前对输入源逐条做连接诊断准入核对：说明采用哪次诊断、为何可用或不可用；未绑定的自由文字输入源仅展示，不猜测绑定。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出ETL任务清单
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
            <th>最近准入</th>
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
              <td>
                {row.最近准入 ? (
                  <button className="link" type="button" onClick={() => setOpenAdmission(row.最近准入 ?? null)}>
                    <span className={row.最近准入.允许启动 ? 'phase-ok' : 'phase-fail'}>
                      {row.最近准入.允许启动 ? '准入通过' : '拒绝准入'}
                    </span>
                  </button>
                ) : (
                  <span className="phase-skip">未核对</span>
                )}
              </td>
              <td className="row-actions">
                <button className="link" type="button" onClick={() => void checkInputs(row)}>输入源准入核对</button>
                <button className="link" type="button" onClick={() => void runAction('提交调度', row)}>提交调度</button>
                <button className="link" type="button" onClick={() => void runAction('启动任务', row)}>启动任务</button>
                <button className="link" type="button" onClick={() => void runAction('终止任务', row)}>终止任务</button>
                <button className="link" type="button" onClick={() => void runAction('归档结果', row)}>归档结果</button>
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 2}>暂无ETL任务数据，可先登记加工任务</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条ETL任务记录</span>
        {notice && <span className="phase-ok">{notice}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>

      {openAdmission && <AdmissionModal admission={openAdmission} onClose={() => setOpenAdmission(null)} />}
    </section>
  )
}

function AdmissionModal({ admission, onClose }: { admission: Admission; onClose: () => void }) {
  return (
    <div className="modal-mask" onClick={onClose}>
      <div className="modal-card modal-wide" onClick={(event) => event.stopPropagation()}>
        <h3>输入源准入核对</h3>
        <p className="page-desc">
          {admission.准入编号 && <>准入编号 {admission.准入编号} · </>}
          {admission.检查时间 && <>{admission.检查时间} · </>}
          总体结论：
          <strong className={admission.允许启动 ? 'phase-ok' : 'phase-fail'}>
            {admission.允许启动 ? '允许启动' : '拒绝准入'}
          </strong>
        </p>
        <table className="data-table">
          <thead>
            <tr><th>输入源</th><th>绑定情况</th><th>判定</th><th>采用诊断</th><th>依据说明</th></tr>
          </thead>
          <tbody>
            {admission.输入源检查.map((check, idx) => (
              <tr key={`${check.输入源文字}-${idx}`}>
                <td>{check.输入源文字}</td>
                <td>{check.绑定 ? `已绑定：${check.配置编号 ?? check.数据源配置ID}` : '未绑定文字'}</td>
                <td className={verdictTone(check.判定)}>{check.判定}</td>
                <td>{check.采用诊断编号 ?? '—'}</td>
                <td>{check.结论}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <p className="page-desc">{admission.说明}</p>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={onClose}>关闭</button>
        </div>
      </div>
    </div>
  )
}
