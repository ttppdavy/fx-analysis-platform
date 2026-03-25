import { NextResponse } from 'next/server'
import { supabaseAdmin } from '@/lib/supabase-server'
import * as XLSX from 'xlsx'

export async function POST(req: Request) {
  try {
    const {
      fileType,
      dataYear,
      tableName,
      storagePaths,
      sourceFilenames,
      bucket,
    } = await req.json()

    const sourceFilenameText = Array.isArray(sourceFilenames)
      ? sourceFilenames.join(' | ')
      : ''

    const { data: batch, error: batchErr } = await supabaseAdmin
      .from('etl_batches')
      .insert({
        file_type: fileType,
        data_year: dataYear,
        table_name: tableName,
        source_filename: sourceFilenameText,
        storage_path: Array.isArray(storagePaths) ? storagePaths.join(' | ') : '',
        status: 'processing',
      })
      .select()
      .single()

    if (batchErr) throw batchErr

    let rowCount = 0

    if (fileType === 'trade') {
      if (!tableName) {
        throw new Error('交易表必须选择表名')
      }

      await supabaseAdmin
        .from('trade_raw_rows')
        .delete()
        .eq('data_year', dataYear)
        .eq('sheet_name', tableName)

      for (const storagePath of storagePaths) {
        const { data: fileData, error: downloadErr } = await supabaseAdmin
          .storage
          .from(bucket)
          .download(storagePath)

        if (downloadErr) throw downloadErr

        const arrayBuffer = await fileData.arrayBuffer()
        const workbook = XLSX.read(arrayBuffer, { type: 'array' })

        const firstSheetName = workbook.SheetNames[0]
        const ws = workbook.Sheets[firstSheetName]
        const rows = XLSX.utils.sheet_to_json(ws, { defval: null })

        const payload = rows.map((row: any, idx: number) => ({
          batch_id: batch.id,
          data_year: dataYear,
          sheet_name: tableName,
          excel_row_num: idx + 2,
          row_data: row,
        }))

        if (payload.length > 0) {
          const { error } = await supabaseAdmin.from('trade_raw_rows').insert(payload)
          if (error) throw error
          rowCount += payload.length
        }
      }
    }

    if (fileType === 'gold_lease') {
      const storagePath = storagePaths[0]

      await supabaseAdmin.from('gold_lease_trades').delete().eq('data_year', dataYear)

      const { data: fileData, error: downloadErr } = await supabaseAdmin
        .storage
        .from(bucket)
        .download(storagePath)

      if (downloadErr) throw downloadErr

      const arrayBuffer = await fileData.arrayBuffer()
      const workbook = XLSX.read(arrayBuffer, { type: 'array' })
      const ws = workbook.Sheets[workbook.SheetNames[0]]
      const rows = XLSX.utils.sheet_to_json(ws, { defval: null }) as any[]

      const payload = rows.map((r) => ({
        batch_id: batch.id,
        data_year: dataYear,
        业务编号: r['业务编号'],
        客户名称: r['客户名称'],
        一级分行: r['一级分行'],
        租借品种: r['租借品种'],
        货物属性: r['货物属性'],
        租借重量_g: r['租借重量（g）'],
        客户计费定盘价: r['客户计费定盘价'],
        同业定盘价: r['同业定盘价'],
        起息日: r['起息日'],
        到期日: r['到期日'],
        天数: r['天数'],
        应付租赁费率: r['应付租赁费率'],
        应收租赁费率: r['应收租赁费率'],
      }))

      if (payload.length > 0) {
        const { error } = await supabaseAdmin.from('gold_lease_trades').insert(payload)
        if (error) throw error
        rowCount += payload.length
      }
    }

    if (fileType === 'crm') {
      const storagePath = storagePaths[0]

      await supabaseAdmin.from('crm_customer_tags').delete().eq('data_year', dataYear)

      const { data: fileData, error: downloadErr } = await supabaseAdmin
        .storage
        .from(bucket)
        .download(storagePath)

      if (downloadErr) throw downloadErr

      const arrayBuffer = await fileData.arrayBuffer()
      const workbook = XLSX.read(arrayBuffer, { type: 'array' })
      const ws = workbook.Sheets[workbook.SheetNames[0]]
      const rows = XLSX.utils.sheet_to_json(ws, { defval: null }) as any[]

      const payload = rows.map((r) => ({
        batch_id: batch.id,
        data_year: dataYear,
        客户名称: r['客户名称'],
        row_data: r,
      }))

      if (payload.length > 0) {
        const { error } = await supabaseAdmin.from('crm_customer_tags').insert(payload)
        if (error) throw error
        rowCount += payload.length
      }
    }

    await supabaseAdmin
      .from('etl_batches')
      .update({
        status: 'success',
        row_count: rowCount,
        finished_at: new Date().toISOString(),
      })
      .eq('id', batch.id)

    return NextResponse.json({ message: `导入成功，${rowCount} 行` })
  } catch (e: any) {
    return NextResponse.json(
      { message: e.message || '导入失败' },
      { status: 500 }
    )
  }
}