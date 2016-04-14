package com.dumbomr.mapred;

import static org.apache.hadoop.hbase.mapreduce.TableRecordReaderImpl.LOG_PER_ROW_COUNT;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.minidev.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerCallable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.apache.hadoop.util.StringUtils;

public class TableRecordReader implements
		RecordReader<TypedBytesWritable, TypedBytesWritable> {
	static final Log LOG = LogFactory.getLog(TableRecordReader.class);
	private byte[] startRow;
	

	private byte[] endRow;
	private byte[] lastSuccessfulRow;
	private Filter trrRowFilter;
	private ResultScanner scanner;
	private HTable htable;
	private byte[][] trrInputColumns;
	private byte[][] trrFamilies;
	private long timestamp;
	private int rowcount;
	private boolean logScannerActivity = false;
	private int logPerRowCount = 100;

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public int getRowcount() {
		return rowcount;
	}

	public void setRowcount(int rowcount) {
		this.rowcount = rowcount;
	}

	public int getLogPerRowCount() {
		return logPerRowCount;
	}

	public void setLogPerRowCount(int logPerRowCount) {
		this.logPerRowCount = logPerRowCount;
	}
	
	public byte[] getLastSuccessfulRow() {
		return lastSuccessfulRow;
	}

	public void setLastSuccessfulRow(byte[] lastSuccessfulRow) {
		this.lastSuccessfulRow = lastSuccessfulRow;
	}

	public Filter getTrrRowFilter() {
		return trrRowFilter;
	}

	public void setTrrRowFilter(Filter trrRowFilter) {
		this.trrRowFilter = trrRowFilter;
	}

	public ResultScanner getScanner() {
		return scanner;
	}

	public void setScanner(ResultScanner scanner) {
		this.scanner = scanner;
	}

	public HTable getHtable() {
		return htable;
	}

	public void setHtable(HTable htable) {
		this.htable = htable;
	}

	public byte[][] getTrrInputColumns() {
		return trrInputColumns;
	}

	public void setTrrInputColumns(byte[][] trrInputColumns) {
		this.trrInputColumns = trrInputColumns;
	}

	public byte[] getStartRow() {
		return startRow;
	}

	public byte[] getEndRow() {
		return endRow;
	}

	public byte[][] getTrrFamilies() {
		return trrFamilies;
	}

	public void setTrrFamilies(byte[][] trrFamilies) {
		this.trrFamilies = trrFamilies;
	}
	
	/**
	 * Restart from survivable exceptions by creating a new scanner.
	 * 
	 * @param firstRow
	 * @throws IOException
	 */
	public void restart(byte[] firstRow) throws IOException {
		Scan currentScan;
		if ((endRow != null) && (endRow.length > 0)) {
			if (trrRowFilter != null) {
				Scan scan = new Scan(firstRow, endRow);
				scan.setMaxVersions(1);
				if (trrFamilies != null) {
					for (byte[] family : trrFamilies) {
						scan.addFamily(family);
					}
				}
				if (trrInputColumns != null) {
					TableInputFormat.addColumns(scan, trrInputColumns);
				}
				scan.setFilter(trrRowFilter);
				scan.setCacheBlocks(false);
				this.scanner = this.htable.getScanner(scan);
				currentScan = scan;
			} else {
				LOG.debug("TIFB.restart, firstRow: "
						+ Bytes.toStringBinary(firstRow) + ", endRow: "
						+ Bytes.toStringBinary(endRow));
				Scan scan = new Scan(firstRow, endRow);
				scan.setMaxVersions(1);
				if (trrFamilies != null) {
					for (byte[] family : trrFamilies) {
						scan.addFamily(family);
					}
				}
				if (trrInputColumns != null) {
					TableInputFormat.addColumns(scan, trrInputColumns);
				}
				this.scanner = this.htable.getScanner(scan);
				currentScan = scan;
			}
		} else {
			LOG.debug("TIFB.restart, firstRow: "
					+ Bytes.toStringBinary(firstRow) + ", no endRow");

			Scan scan = new Scan(firstRow);
			scan.setMaxVersions(1);
			if (trrFamilies != null) {
				for (byte[] family : trrFamilies) {
					scan.addFamily(family);
				}
			}
			if (trrInputColumns != null) {
				TableInputFormat.addColumns(scan, trrInputColumns);
			}
//			TableInputFormat.addColumns(scan, trrInputColumns);
			scan.setFilter(trrRowFilter);
			this.scanner = this.htable.getScanner(scan);
			currentScan = scan;
		}
		if (logScannerActivity) {
			LOG.info("Current scan=" + currentScan.toString());
			timestamp = System.currentTimeMillis();
			rowcount = 0;
		}
	}

	/**
	 * Build the scanner. Not done in constructor to allow for extension.
	 * 
	 * @throws IOException
	 */
	public void init() throws IOException {
		restart(startRow);
	}

	/**
	 * @param htable
	 *            the {@link HTable} to scan.
	 */
	public void setHTable(HTable htable) {
		Configuration conf = htable.getConfiguration();
	    logScannerActivity = conf.getBoolean(
	      ScannerCallable.LOG_SCANNER_ACTIVITY, false);
	    logPerRowCount = conf.getInt(LOG_PER_ROW_COUNT, 100);
	    this.htable = htable;
	}

	/**
	 * @param inputColumns
	 *            the columns to be placed in {@link RowResult}.
	 */
	public void setInputColumns(final byte[][] inputColumns) {
		this.trrInputColumns = inputColumns;
	}

	/**
	 * @param startRow
	 *            the first row in the split
	 */
	public void setStartRow(final byte[] startRow) {
		this.startRow = startRow;
	}

	/**
	 * @param endRow
	 *            the last row in the split
	 */
	public void setEndRow(final byte[] endRow) {
		this.endRow = endRow;
	}

	/**
	 * @param rowFilter
	 *            the {@link RowFilterInterface} to be used.
	 */
	public void setRowFilter(Filter rowFilter) {
		this.trrRowFilter = rowFilter;
	}

	public void close() {
		this.scanner.close();
	    try {
	      this.htable.close();
	    } catch (IOException ioe) {
	      LOG.warn("Error closing table", ioe);
	    }
	}

	/**
	 * @return ImmutableBytesWritable
	 * @see org.apache.hadoop.mapred.RecordReader#createKey()
	 */
	public TypedBytesWritable createKey() {
		return new TypedBytesWritable();
	}

	/**
	 * @return RowResult
	 * @see org.apache.hadoop.mapred.RecordReader#createValue()
	 */
	public TypedBytesWritable createValue() {
		return new TypedBytesWritable();
	}

	public long getPos() {
		// This should be the ordinal tuple in the range;
		// not clear how to calculate...
		return 0;
	}

	public float getProgress() {
		// Depends on the total number of tuples and getPos
		return 0;
	}

	/**
	 * @param key
	 *            HStoreKey as input key.
	 * @param value
	 *            MapWritable as input value
	 * @return true if there was more data
	 * @throws IOException
	 */
	public String resultFormatter(Result result) {
		  Cell[] cells = result.rawCells();
		  // family -> column family -> qualifier
		  JSONObject familyMap = new JSONObject();
		  
		  Map<String, Long> cellTime = new HashMap<String, Long>();
		  
		  for(Cell kv : cells) {
		      byte [] family = CellUtil.cloneFamily(kv);
		      String familyStr = Bytes.toString(family);
		      JSONObject columnMap = (JSONObject) familyMap.get(familyStr);
		      if(columnMap == null) {
		        columnMap = new JSONObject();
		        familyMap.put(familyStr, columnMap);
		      }
		      byte [] qualifier = CellUtil.cloneQualifier(kv);
		      String qualifierStr = Bytes.toString(qualifier);
		      
		      byte [] value = CellUtil.cloneValue(kv);
		      columnMap.put(qualifierStr, Bytes.toString(value));
		  }
		  
		  return familyMap.toJSONString();
	  }
	
	public boolean next(TypedBytesWritable key, TypedBytesWritable value)
			throws IOException {
		Result result;
	    try {
	      try {
	        result = this.scanner.next();
	        if (logScannerActivity) {
	          rowcount ++;
	          if (rowcount >= logPerRowCount) {
	            long now = System.currentTimeMillis();
	            LOG.info("Mapper took " + (now-timestamp)
	              + "ms to process " + rowcount + " rows");
	            timestamp = now;
	            rowcount = 0;
	          }
	        }
	      } catch (IOException e) {
	        // try to handle all IOExceptions by restarting
	        // the scanner, if the second call fails, it will be rethrown
	        LOG.debug("recovered from " + StringUtils.stringifyException(e));
	        if (lastSuccessfulRow == null) {
	          LOG.warn("We are restarting the first next() invocation," +
	              " if your mapper has restarted a few other times like this" +
	              " then you should consider killing this job and investigate" +
	              " why it's taking so long.");
	        }
	        if (lastSuccessfulRow == null) {
	          restart(startRow);
	        } else {
	          restart(lastSuccessfulRow);
	          this.scanner.next();    // skip presumed already mapped row
	        }
	        result = this.scanner.next();
	      }

	      if (result != null && result.size() > 0) {
	        key.setValue(Bytes.toString(result.getRow()));
	        lastSuccessfulRow = result.getRow();
	        value.setValue(resultFormatter(result));
//	        System.out.println("PC-ROW:" +  result.getRow() + ", PC-VALUE: " + value.toString());
	        return true;
	      }
	      return false;
	    } catch (IOException ioe) {
	      if (logScannerActivity) {
	        long now = System.currentTimeMillis();
	        LOG.info("Mapper took " + (now-timestamp)
	          + "ms to process " + rowcount + " rows");
	        LOG.info(ioe);
	        String lastRow = lastSuccessfulRow == null ?
	          "null" : Bytes.toStringBinary(lastSuccessfulRow);
	        LOG.info("lastSuccessfulRow=" + lastRow);
	      }
	      throw ioe;
	    }
	  }
}
