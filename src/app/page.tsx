'use client'

import { useState } from 'react'
import { supabase } from '@/lib/supabase'

export default function HomePage() {
  const [year, setYear] = useState('2026')
  const [fileType, setFileType] = useState('trade')
  const [file, setFile] = useState<File | null>(null)
  const [msg, setMsg] = useState('')

  const bucketMap: Record<string, string> = {
    trade: 'trade-files',
    gold_lease: 'gold-files',
    crm: 'crm-files',
  }

  async function handleUpload() {
    if (!file) {
      setMsg('请先选择文件')
      return
    }

    const bucket = bucketMap[fileType]
    const filePath = `${year}/${Date.now()}_${file.name}`

    const { error } = await supabase.storage
      .from(bucket)
      .upload(filePath, file, { upsert: true })

    if (error) {
      setMsg(`上传失败：${error.message}`)
      return
    }

    const res = await fetch('/api/import', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        fileType,
        dataYear: Number(year),
        storagePath: filePath,
        sourceFilename: file.name,
        bucket,
      }),
    })

    const data = await res.json()
    setMsg(data.message || '完成')
  }

  return (
    <main style={{ padding: 24 }}>
      <h1>数据上传</h1>

      <div style={{ marginBottom: 12 }}>
        <label>年份：</label>
        <select value={year} onChange={(e) => setYear(e.target.value)}>
          <option value="2025">2025</option>
          <option value="2026">2026</option>
        </select>
      </div>

      <div style={{ marginBottom: 12 }}>
        <label>文件类型：</label>
        <select value={fileType} onChange={(e) => setFileType(e.target.value)}>
          <option value="trade">交易表</option>
          <option value="gold_lease">黄金租赁</option>
          <option value="crm">CRM</option>
        </select>
      </div>

      <div style={{ marginBottom: 12 }}>
        <input
          type="file"
          accept=".xlsx,.xlsm"
          onChange={(e) => setFile(e.target.files?.[0] || null)}
        />
      </div>

      <button onClick={handleUpload}>上传并导入</button>

      <p>{msg}</p>
    </main>
  )
}