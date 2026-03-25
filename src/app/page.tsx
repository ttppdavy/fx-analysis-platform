'use client'

import { useState } from 'react'
import { supabase } from '@/lib/supabase-client'
import * as tus from 'tus-js-client'

const TRADE_TABLES = ['即汇通', '其他交易表']

export default function HomePage() {
  const [year, setYear] = useState('2026')
  const [fileType, setFileType] = useState('trade')
  const [tableName, setTableName] = useState('即汇通')
  const [files, setFiles] = useState<File[]>([])
  const [msg, setMsg] = useState('')
  const [progress, setProgress] = useState(0)
  const [uploading, setUploading] = useState(false)
  const [importing, setImporting] = useState(false)

  const bucketMap: Record<string, string> = {
    trade: 'trade-files',
    gold_lease: 'gold-files',
    crm: 'crm-files',
  }

  function safeFileName(name: string) {
    const extIndex = name.lastIndexOf('.')
    const ext = extIndex >= 0 ? name.slice(extIndex) : ''
    const base = extIndex >= 0 ? name.slice(0, extIndex) : name

    const cleaned = base
      .replace(/[^\w\-]+/g, '_')
      .replace(/_+/g, '_')
      .replace(/^_+|_+$/g, '')

    return `${cleaned || 'file'}${ext.toLowerCase()}`
  }

  function handleFiles(selected: FileList | null) {
    if (!selected) return
    const arr = Array.from(selected)

    if (fileType === 'trade') {
      if (tableName === '即汇通') {
        if (arr.length > 3) {
          setMsg('即汇通一次最多上传 3 个文件')
          return
        }
      } else {
        if (arr.length > 1) {
          setMsg('其他交易表一次只能上传 1 个文件')
          return
        }
      }
    }

    if (fileType !== 'trade' && arr.length > 1) {
      setMsg('黄金租赁和 CRM 一次只能上传 1 个文件')
      return
    }

    setMsg('')
    setFiles(arr)
  }

  async function uploadOneFile(
    file: File,
    bucket: string,
    filePath: string,
    index: number,
    total: number,
  ) {
    return new Promise<string>((resolve, reject) => {
      const directStorageUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!.replace(
        '.supabase.co',
        '.storage.supabase.co',
      )

      const upload = new tus.Upload(file, {
        endpoint: `${directStorageUrl}/storage/v1/upload/resumable`,
        retryDelays: [0, 1000, 3000, 5000],
        headers: {
          authorization: `Bearer ${process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY}`,
          'x-upsert': 'true',
        },
        metadata: {
          bucketName: bucket,
          objectName: filePath,
          contentType: file.type || 'application/octet-stream',
          cacheControl: '3600',
        },
        uploadDataDuringCreation: true,
        removeFingerprintOnSuccess: true,
        chunkSize: 6 * 1024 * 1024,
        onError(error) {
          reject(error)
        },
        onProgress(bytesUploaded, bytesTotal) {
          const currentFilePercent = bytesTotal > 0 ? bytesUploaded / bytesTotal : 0
          const totalPercent = Math.floor(((index + currentFilePercent) / total) * 100)
          setProgress(totalPercent)
        },
        onSuccess() {
          resolve(filePath)
        },
      })

      upload.start()
    })
  }

  async function handleUpload() {
    if (files.length === 0) {
      setMsg('请先选择文件')
      return
    }

    setMsg('')
    setProgress(0)
    setUploading(true)
    setImporting(false)

    try {
      const bucket = bucketMap[fileType]
      const storagePaths: string[] = []

      for (let i = 0; i < files.length; i++) {
        const file = files[i]
        const filePath = `${year}/${Date.now()}_${i + 1}_${safeFileName(file.name)}`
        const savedPath = await uploadOneFile(file, bucket, filePath, i, files.length)
        storagePaths.push(savedPath)
      }

      setUploading(false)
      setImporting(true)
      setMsg('上传完成，开始导入数据库...')

      const res = await fetch('/api/import', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          fileType,
          dataYear: Number(year),
          tableName: fileType === 'trade' ? tableName : null,
          storagePaths,
          sourceFilenames: files.map((f) => f.name),
          bucket,
        }),
      })

      const data = await res.json()
      setImporting(false)

      if (!res.ok) {
        setMsg(data.message || '导入失败')
        return
      }

      setMsg(data.message || '上传并导入完成')
      setFiles([])
      setProgress(100)
    } catch (e: any) {
      setUploading(false)
      setImporting(false)
      setMsg(`失败：${e.message || '未知错误'}`)
    }
  }

  return (
    <main
      style={{
        maxWidth: 820,
        margin: '0 auto',
        padding: 24,
        fontFamily: 'Arial, sans-serif',
      }}
    >
      <h1 style={{ fontSize: 30, marginBottom: 24 }}>数据上传</h1>

      <div
        style={{
          border: '1px solid #ddd',
          borderRadius: 14,
          padding: 24,
          background: '#fff',
          boxShadow: '0 2px 8px rgba(0,0,0,0.05)',
        }}
      >
        <div style={{ marginBottom: 16 }}>
          <label style={{ display: 'inline-block', width: 90 }}>年份：</label>
          <select value={year} onChange={(e) => setYear(e.target.value)}>
            <option value="2025">2025</option>
            <option value="2026">2026</option>
          </select>
        </div>

        <div style={{ marginBottom: 16 }}>
          <label style={{ display: 'inline-block', width: 90 }}>文件类型：</label>
          <select
            value={fileType}
            onChange={(e) => {
              setFileType(e.target.value)
              setFiles([])
              setMsg('')
            }}
          >
            <option value="trade">交易表</option>
            <option value="gold_lease">黄金租赁</option>
            <option value="crm">CRM</option>
          </select>
        </div>

        {fileType === 'trade' && (
          <div style={{ marginBottom: 16 }}>
            <label style={{ display: 'inline-block', width: 90 }}>交易类型：</label>
            <select
              value={tableName}
              onChange={(e) => {
                setTableName(e.target.value)
                setFiles([])
                setMsg('')
              }}
            >
              {TRADE_TABLES.map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </div>
        )}

        <div
          style={{
            marginBottom: 16,
            padding: 12,
            background: '#f7f7f7',
            borderRadius: 8,
            fontSize: 14,
            color: '#555',
            lineHeight: 1.6,
          }}
        >
          {fileType === 'trade'
            ? tableName === '即汇通'
              ? '即汇通：一次最多上传 3 个文件，系统会自动合并导入。'
              : '其他交易表：请放在 1 个 xlsx 文件里上传。'
            : fileType === 'gold_lease'
              ? '黄金租赁：一次上传 1 个文件。'
              : 'CRM：一次上传 1 个文件。'}
        </div>

        <div style={{ marginBottom: 16 }}>
          <input
            type="file"
            accept=".xlsx,.xlsm"
            multiple
            onChange={(e) => handleFiles(e.target.files)}
          />
        </div>

        {files.length > 0 && (
          <div
            style={{
              marginBottom: 16,
              padding: 12,
              background: '#fafafa',
              border: '1px solid #eee',
              borderRadius: 8,
              fontSize: 14,
            }}
          >
            <div style={{ marginBottom: 8, fontWeight: 700 }}>已选文件：</div>
            {files.map((file, idx) => (
              <div key={idx} style={{ marginBottom: 4 }}>
                {idx + 1}. {file.name}（{(file.size / 1024 / 1024).toFixed(2)} MB）
              </div>
            ))}
          </div>
        )}

        {(uploading || importing) && (
          <div style={{ marginBottom: 16 }}>
            <div
              style={{
                width: '100%',
                height: 16,
                background: '#eee',
                borderRadius: 8,
                overflow: 'hidden',
                marginBottom: 8,
              }}
            >
              <div
                style={{
                  width: `${progress}%`,
                  height: '100%',
                  background: '#1677ff',
                  transition: 'width 0.2s',
                }}
              />
            </div>
            <div style={{ fontSize: 14, color: '#333' }}>
              {uploading ? `上传中：${progress}%` : '正在导入数据库，请稍等...'}
            </div>
          </div>
        )}

        <button
          onClick={handleUpload}
          disabled={files.length === 0 || uploading || importing}
          style={{
            padding: '10px 18px',
            borderRadius: 8,
            border: 'none',
            background: uploading || importing ? '#999' : '#1677ff',
            color: '#fff',
            cursor: uploading || importing ? 'not-allowed' : 'pointer',
            fontSize: 15,
          }}
        >
          {uploading ? '上传中...' : importing ? '导入中...' : '上传并导入'}
        </button>

        {msg && (
          <div
            style={{
              marginTop: 16,
              padding: 12,
              borderRadius: 8,
              background: '#f8f8f8',
              fontSize: 14,
              color: '#333',
              whiteSpace: 'pre-wrap',
            }}
          >
            {msg}
          </div>
        )}
      </div>
    </main>
  )
}