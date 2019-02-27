package org.apache.gdr.common;

import static org.junit.Assert.assertEquals;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.gdr.common.datatype.AbDataType;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.schema.column.AbColumnDef;
import org.apache.gdr.common.schema.dataset.AbDataDef;
import org.apache.gdr.common.util.AbUtils;
import org.apache.gdr.common.util.DmlParser;

import junit.framework.Assert;

public class DmlTest {
	static Log LOG = LogFactory.getLog(DmlTest.class);

	static AbDataDef abSchema;
	static BufferedReader br;
	static File testDir = null;

	static int dmlCols = 0;
	static Map<String, String> inputMap = new HashMap<>();
	static {
	}

	@BeforeClass
	public static void init() {
		testDir = new File("target/dat");
		if (!testDir.exists())
			testDir.mkdir();
	}

	@SuppressWarnings({ "deprecation", "resource" })
	private static Integer getDmlCols(String dmlFile2) throws IOException {
		DataInputStream fis = new DataInputStream(new BufferedInputStream(
				new FileInputStream(new File(dmlFile2))));
		int cnt = 0;
		while (fis.available() > 0) {
			String s = fis.readLine();
			if (!StringUtils.containsIgnoreCase(s, "record")
					&& !s.trim().equalsIgnoreCase("end"))
				cnt++;
		}
		return cnt;
	}

	static AbDataDef getDef(String fp) throws IOException, GdrException {
		File f = new File(fp);
		br = new BufferedReader(new FileReader(f));
		StringBuilder sb = new StringBuilder();

		int i;
		while ((i = br.read()) >= 0) {
			sb.append((char) i);
		}

		String name = f.getName().replace(".dml", "");
		String[] strs = name.split("\\.", 3);
		if (strs.length > 1) {
			name = strs[1];
		} else {
			name = strs[0];
		}

		if (br != null)
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		return DmlParser.parseDml(name, sb.toString());
	}

	@Test
	public void TestCols() throws IOException, GdrException {
		for (Entry<String, String> kv : inputMap.entrySet()) {
			abSchema = getDef(kv.getKey());
			dmlCols = getDmlCols(kv.getKey());
			assertEquals(dmlCols, abSchema.getColumnList().size());
		}
	}

	@Test
	public void TestColDef() throws IOException, GdrException {
		for (Entry<String, String> kv : inputMap.entrySet()) {
			abSchema = getDef(kv.getKey());
			dmlCols = getDmlCols(kv.getKey());
			int i = 1;
			for (AbColumnDef col : abSchema.getColumnList()) {
				LOG.info("Column " + i + ": " +col.toString());
				i++;
			}
		}
	}

	@Test
	public void TestAvroShhema() throws IOException, GdrException {
		for (Entry<String, String> kv : inputMap.entrySet()) {
			abSchema = getDef(kv.getKey());
			LOG.info(abSchema.getAvroSchema());
		}
	}

	@Test
	public void TestRead() throws IOException, GdrException {
		for (Entry<String, String> kv : inputMap.entrySet()) {
			LOG.info(" working reread on file: " + kv.getValue());
			abSchema = getDef(kv.getKey());
			BufferedInputStream fis = new BufferedInputStream(new FileInputStream(new File(kv.getValue())));

			AbRecord rec = new AbRecord(abSchema);
			long l =0;
			while (rec.read(fis)) {
			    if (rec.getData().get("trn_introduced")==null)
			        l++;
			}
			LOG.info(l);
			fis.close();
		}
	}

	@Test
	public void TestWriteAvroFile() throws IOException, GdrException {
		for (Entry<String, String> kv : inputMap.entrySet()) {
			LOG.info(" working reread on file: " + kv.getValue());
			abSchema = getDef(kv.getKey());
			BufferedInputStream fis = new BufferedInputStream(new FileInputStream(new File(kv.getValue())));
			String fn = testDir.getPath() + "/"
					+ kv.getValue().substring(kv.getValue().lastIndexOf("/"), kv.getValue().lastIndexOf("."))
					+ ".avro";
			File of = new File(fn);
			testDir.getPath();
			DatumWriter<GenericRecord> writer=
					new GenericDatumWriter<GenericRecord>(abSchema.genAvroSchema(true, true));
			DataFileWriter<GenericRecord> fo = new DataFileWriter<GenericRecord>(writer);
			fo.create(abSchema.genAvroSchema(true, true), of);

			AbRecord rec = new AbRecord(abSchema);
			while (rec.read(fis)) {
				fo.append(rec.getData());
			}
			LOG.info("data read done.");

			fo.close();
			fis.close();
		}
	}

	@Test
	public void TestWriteJsonFile() throws IOException, GdrException {
		for (Entry<String, String> kv : inputMap.entrySet()) {
			LOG.info(" working reread on file: " + kv.getValue());
			abSchema = getDef(kv.getKey());
			dmlCols = getDmlCols(kv.getKey());
			BufferedInputStream fis = new BufferedInputStream(new FileInputStream(new File(kv.getValue())));

			String fn = testDir.getPath() + "/"
					+ kv.getValue().substring(kv.getValue().lastIndexOf("/"), kv.getValue().lastIndexOf("."))
					+ ".json";
			File of = new File(fn);
			BufferedOutputStream fo = new BufferedOutputStream(new FileOutputStream(of));

			AbRecord rec = new AbRecord(abSchema);
			while (rec.read(fis)) {
				fo.write(rec.getData().toString().getBytes());
				fo.write('\n');
			}
			LOG.info("data read done.");

			fo.close();
			fis.close();
		}
	}

	@Test
	public void TestWriteTextFile() throws IOException, GdrException {
		for (Entry<String, String> kv : inputMap.entrySet()) {
			LOG.info(" working reread on file: " + kv.getValue());
			abSchema = getDef(kv.getKey());
			dmlCols = getDmlCols(kv.getKey());
			BufferedInputStream fis = new BufferedInputStream(new FileInputStream(new File(kv.getValue())));

			String fn = testDir.getPath() + "/"
					+ kv.getValue().substring(kv.getValue().lastIndexOf("/"), kv.getValue().lastIndexOf("."))
					+ ".txt";
			File of = new File(fn);
			BufferedOutputStream fo = new BufferedOutputStream(new FileOutputStream(of));
			//FileOutputStream fo = new FileOutputStream(of);

			AbRecord  ab = new AbRecord(abSchema);
			while (ab.read(fis)) {
				fo.write(ab.toString().getBytes());
				fo.write('\n');
			}
			LOG.info("data read done.");

			fo.close();
			fis.close();
		}
	}

	@Test
	public void TestBitMap() throws IOException, GdrException {
		for (Entry<String, String> kv : inputMap.entrySet()) {
			LOG.info(" working reread on file: " + kv.getValue());
			abSchema = getDef(kv.getKey());
			dmlCols = getDmlCols(kv.getKey());
			LOG.info(" working reread on file: " + kv.getValue());
			BufferedInputStream fis = new BufferedInputStream(new FileInputStream(new File(kv.getValue())));
			AbRecord data = null;
			Map<String, Long> headerMap = new HashMap<>();
			Map<String, Set<String>> headerComp = new HashMap<>();

			// print nullable column
			LOG.info("bitmap 1:");
			int hh = 0;
			for (AbColumnDef col : abSchema.getColumnList()) {
				if (col.isNullable() && col.getNVL() == null) {
					LOG.info(hh + ":" + col.getName());
					hh++;
				}
			}

			while (fis.available() > 0) {
				data = new AbRecord(abSchema);
				byte[] header0 = new byte[abSchema.getHeaderLen()];
				data.read(fis);

				header0 = data.genHeader();
				if (!AbUtils.compareByteArray(data.getHeader(), header0)) {
					header0 = data.genHeader();
					LOG.info(AbUtils.bytes2BitString(data.getHeader()));
					LOG.info(AbUtils.bytes2BitString(header0));
				}
				//Assert.assertTrue(AbUtils.compareByteArray(data.getHeader(), header0));
				headerMap.put(AbUtils.bytes2BitString(data.getHeader())
						, headerMap.containsKey(AbUtils.bytes2BitString(data.getHeader()))
						? headerMap.get(AbUtils.bytes2BitString(data.getHeader())) + 1 : 1);
				if (!headerComp.containsKey(AbUtils.bytes2BitString(data.getHeader())))
					headerComp.put(AbUtils.bytes2BitString(data.getHeader()), new HashSet<String>());
				headerComp.get(AbUtils.bytes2BitString(data.getHeader()))
				.add(AbUtils.bytes2BitString(header0));
			}
			fis.close();

			for (Entry<String, Set<String>> o : headerComp.entrySet()) {
				System.out.print(String.format("%s [", o.getKey()));
				for (String v : o.getValue()) {
					System.out.print(v.trim());
					System.out.print("(" + (v.equals(o.getKey()) ? "T" : "F") + ")");
				}
				System.out.println("] " + headerMap.get(o.getKey()));
			}
		}
	}

	@Test
	public void TestReread() throws NoSuchAlgorithmException, IOException, GdrException {
		for (Entry<String, String> kv : inputMap.entrySet()) {
			abSchema = getDef(kv.getKey());
			dmlCols = getDmlCols(kv.getKey());
			LOG.info(" working reread on file: " + kv.getValue());
			BufferedInputStream fis = new BufferedInputStream(new FileInputStream(new File(kv.getValue())));
			AbRecord data = new AbRecord(abSchema);
			BufferedOutputStream dout;

			boolean enableNVL = true;
			boolean reGenHead = true;

			String outFile = testDir.getPath() + "/"
					+ kv.getValue().substring(kv.getValue().lastIndexOf("/"), kv.getValue().lastIndexOf("."))
					+ ".out.bin";
			String finalFile = testDir.getPath() + "/"
					+ kv.getValue().substring(kv.getValue().lastIndexOf("/"), kv.getValue().lastIndexOf("."))
					+ ".final.bin";

			dout = new BufferedOutputStream(new FileOutputStream(new File(outFile)));
			int i = 0;
			try {
				while (fis.available() > 0) {
					i++;
					data.read(fis);
					data.write(dout, enableNVL, reGenHead);
				}
			}catch (GdrException e) {
				LOG.info("error on line: " + i);
				e.printStackTrace();
			}

			dout.close();
			fis.close();

			FileInputStream is = new FileInputStream(outFile);
			MessageDigest md = MessageDigest.getInstance("MD5");
			String digOut = getDigest(is, md, 2048);
			is.close();
			is = new FileInputStream(kv.getValue());
			md = MessageDigest.getInstance("MD5");
			String digIn = getDigest(is, md, 2048);
			is.close();

			DataInputStream d1= null, d0 = null;
			if (!digIn.equals(digOut)) {
				LOG.info("md5 diff between " + kv.getValue() + " and " + outFile + " checking content now:");
				try {
					d1 = new DataInputStream(new FileInputStream(outFile));
					d0 = new DataInputStream(new FileInputStream(kv.getValue()));
					i = 0;
					while (d0.available() > 0) {
						i++;
						byte[] s0 = new byte[100];
						d0.read(s0);
						byte[] s1 = new byte[100];
						d1.read(s1);
						if (!AbUtils.compareByteArray(s0,s1))
							LOG.debug(Assert.format("diff on pos " + i*100 + ":", new String(s0), new String(s1)));
						//skip hide null
						if (!abSchema.hasHideNull() && !hasBinaryCol(abSchema))
							assertEquals("diff on pos " + i*100 + ":", new String(s0), new String(s1));
					}

				} catch (IOException e1) {
					throw e1;
				} finally {
					if (d1 != null) d1.close();
					if (d0 != null) d0.close();
				}
			}

			// test output file
			fis = new BufferedInputStream(new FileInputStream(new File(outFile)));
			dout = new BufferedOutputStream(new FileOutputStream(new File(finalFile)));
			while (fis.available() > 0) {
				data.read(fis);
				data.write(dout, enableNVL, reGenHead);
			}

			dout.close();
			fis.close();

			is = new FileInputStream(finalFile);
			md = MessageDigest.getInstance("MD5");
			String digFin = getDigest(is, md, 2048);
			is.close();

			if (!digOut.equals(digFin)) {
				LOG.info("md5 diff between " + outFile + " and " + finalFile + " checking contect now:");
				try {
					d1 = new DataInputStream(new FileInputStream(outFile));
					d0 = new DataInputStream(new FileInputStream(finalFile));
					i = 0;
					while (d0.available() > 0) {
						i++;
						byte[] s0 = new byte[100];
						d0.read(s0);
						byte[] s1 = new byte[100];
						d1.read(s1);
						if (!AbUtils.compareByteArray(s0,s1))
							LOG.debug(Assert.format("diff on pos " + i*100 + ":", new String(s0), new String(s1)));
						if (!abSchema.hasHideNull() && hasBinaryCol(abSchema))
							assertEquals("diff on pos " + i*100 + ":", new String(s0), new String(s1));
					}
				} catch (Exception e1) {
					throw e1;
				} finally {
					if (d1 != null) d1.close();
					if (d0 != null) d0.close();
				}
			}
		}
	}

	private Boolean hasBinaryCol(AbDataDef abSchema) {
		if (abSchema == null)
			return null;

		for (int i = 0; i < abSchema.getColumnList().size(); i++) {
			if (abSchema.getColumn(i).getType().equals(AbDataType.VOID)
					|| abSchema.getColumn(i).getType().equals(AbDataType.INTEGER)
					|| abSchema.getColumn(i).getType().equals(AbDataType.REAL)
					|| abSchema.getColumn(i).hasBinaryHeader())
				return true;
		}
		return false;
	}

	//@Test
	//@SuppressWarnings("restriction")
	public void testDumb() throws IOException, GdrException {
		/*
		String delimiter = "ple";
		int delimiterLen = delimiter.getBytes().length;

		ByteBuffer bb = ByteBuffer.allocateDirect(1024 * 1024 * 1024);
		bb.put("中国".getBytes(StandardCharsets.UTF_8)).put(" people".getBytes(StandardCharsets.UTF_8));
		int i = bb.position();
		LOG.info(i);
		byte[] temp = new byte[delimiterLen];
		LOG.info((char) bb.get(i - delimiterLen));
		if (compareBufferEnd(delimiter, bb)) {
			temp = new byte[bb.position() - delimiterLen];
			int len = bb.position();
			bb.flip();
			bb.get(temp, 0, len - delimiterLen);
		}
		LOG.info(new String(temp, StandardCharsets.UTF_8));
		if (bb.isDirect()) {
			((sun.nio.ch.DirectBuffer) bb).cleaner().clean();
		}

		byte[] b1 = new byte[] { 0, 0 };
		AbUtils.setBit(b1, 8, true);
		LOG.info(Integer.MAX_VALUE);
		*/
		String s = "123.00";
		Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();
		Schema colSchema = LogicalTypes.decimal(18, 2)
				.addToSchema(Schema.create(AbUtils.abTypeToAvroTypeV2(AbDataType.DECIMAL, 18)));
		BigDecimal l = new BigDecimal(s.trim());
		ByteBuffer bb = conversion.toBytes(l, null, colSchema.getLogicalType());

		String s1 = "123.00";
		Conversions.DecimalConversion conversion1 = new Conversions.DecimalConversion();
		Schema colSchema1 = LogicalTypes.decimal(18, 2)
				.addToSchema(Schema.create(AbUtils.abTypeToAvroTypeV2(AbDataType.DECIMAL, 18)));
		BigDecimal l1 = new BigDecimal(s.trim());
		ByteBuffer bb1 = conversion.toBytes(l1, null, colSchema.getLogicalType());


		System.out.println(bb.equals(bb1));
	}

	boolean compareBufferEnd(String delimiter, ByteBuffer bb) {
		byte[] bytes = delimiter.getBytes();
		for (int i = 0; i < bytes.length; i++) {
			if (bytes[i] != bb.get(bb.position() - bytes.length + i)) {
				return false;
			}
		}
		return true;
	}

	public static String getDigest(InputStream is, MessageDigest md, int byteArraySize)
			throws NoSuchAlgorithmException, IOException {

		md.reset();
		byte[] bytes = new byte[byteArraySize];
		int numBytes;
		while ((numBytes = is.read(bytes)) != -1) {
			md.update(bytes, 0, numBytes);
		}
		byte[] digest = md.digest();
		String result = new String(Hex.encodeHex(digest));
		return result;
	}

	@AfterClass
	public static void tearDown() throws IOException {
	}
}
