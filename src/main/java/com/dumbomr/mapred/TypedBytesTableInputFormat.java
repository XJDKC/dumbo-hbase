package com.dumbomr.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.mapred.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.apache.hadoop.util.StringUtils;

public class TypedBytesTableInputFormat implements
		InputFormat<TypedBytesWritable, TypedBytesWritable>, JobConfigurable {
	private final Log LOG = LogFactory.getLog(TypedBytesTableInputFormat.class);
	public static final String COLUMN_LIST = "hbase.mapred.tablecolumns";
	public static final String ZK_HOST = "hbase.zookeeper.quorum";
	public static final String FAMILY_LIST = "columnfamilies";
	
	private byte[][] inputColumns = null;
	private HTable table;
	private TableRecordReader tableRecordReader;

	private Filter rowFilter;

	private byte[][] families = null;

	protected void setInputColumns(byte[][] inputColumns) {
		this.inputColumns = inputColumns;
	}

	protected HTable getHTable() {
		return this.table;
	}

	protected void setHTable(HTable table) {
		this.table = table;
	}

	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		Path[] tableNames = FileInputFormat.getInputPaths(job);
		String colArg = job.get(COLUMN_LIST);
		String familyArg = job.get(FAMILY_LIST);
		String zk_config = job.get(ZK_HOST);
		
		
		Configuration config = HBaseConfiguration.create(job);
		
		config.setInt("hbase.client.scanner.caching", 500);
		
		if (zk_config != null) {
	    	config.set(ZK_HOST, zk_config);
	    }

		if (colArg != null) {
			String[] colNames = colArg.split(",");
			
			byte[][] m_cols = new byte[colNames.length][];
			for (int i = 0; i < m_cols.length; i++) {
				m_cols[i] = Bytes.toBytes(colNames[i]);
			}
			setInputColumns(m_cols);
		}
		
		if (familyArg != null) {
			String[] familyNames = familyArg.split(",");
			
			byte[][] families = new byte[familyNames.length][];
			for (int i = 0; i < families.length; i++) {
				families[i] = Bytes.toBytes(familyNames[i]);
			}
			setFamilies(families);
		}
		
		try {
			setHTable(new HTable(config, tableNames[0].getName()));
		} catch (Exception e) {
			LOG.error(StringUtils.stringifyException(e));
		}
	}

	@SuppressWarnings("deprecation")
	public RecordReader<TypedBytesWritable, TypedBytesWritable> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		TableSplit tSplit = (TableSplit) split;
		TableRecordReader trr = this.tableRecordReader;
		// if no table record reader was provided use default
		if (trr == null) {
			trr = new TableRecordReader();
		}
		trr.setStartRow(tSplit.getStartRow());
		trr.setEndRow(tSplit.getEndRow());
		trr.setHTable(this.table);
		trr.setInputColumns(this.inputColumns);
		trr.setTrrFamilies(this.families);
		trr.setRowFilter(this.rowFilter);
		trr.init();
		return trr;
	}

	@SuppressWarnings("deprecation")
	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException {
		if (this.table == null) {
			throw new IOException("No table was provided");
		}
		byte[][] startKeys = this.table.getStartKeys();
		if (startKeys == null || startKeys.length == 0) {
			throw new IOException("Expecting at least one region");
		}
		if (this.inputColumns == null && this.families == null) {
			throw new IOException("Expecting at least one column");
		}
		int realNumSplits = numSplits > startKeys.length ? startKeys.length
				: numSplits;
		InputSplit[] splits = new InputSplit[realNumSplits];
		int middle = startKeys.length / realNumSplits;
		int startPos = 0;
		for (int i = 0; i < realNumSplits; i++) {
			int lastPos = startPos + middle;
			lastPos = startKeys.length % realNumSplits > i ? lastPos + 1
					: lastPos;
			String regionLocation = table
					.getRegionLocation(startKeys[startPos]).getHostname();
			splits[i] = new TableSplit(this.table.getTableName(),
					startKeys[startPos],
					((i + 1) < realNumSplits) ? startKeys[lastPos]
							: HConstants.EMPTY_START_ROW, regionLocation);
			LOG.info("split: " + i + "->" + splits[i]);
			startPos = lastPos;
		}
		return splits;
	}

	public byte[][] getFamilies() {
		return families;
	}

	public void setFamilies(byte[][] families) {
		this.families = families;
	}
	

	public TableRecordReader getTableRecordReader() {
		return tableRecordReader;
	}

	public void setTableRecordReader(TableRecordReader tableRecordReader) {
		this.tableRecordReader = tableRecordReader;
	}

	public Filter getRowFilter() {
		return rowFilter;
	}

	public void setRowFilter(Filter rowFilter) {
		this.rowFilter = rowFilter;
	}
}
