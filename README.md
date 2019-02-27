# Genaral Data Router
### A tool to help big data platform(Hadoop/Hive/Spark) to consume/generate binary data without coding.

Binary data format is widely used in traditional IO processes, many legacy platform and tools, data will be serialized to byte flow and persistent to file system as binary files.
In big data world, to benefit from parallel processes,  data are stored as "Splitable", so data can be processed in more "pipes" instead of the limit of file numbers.
This project is to provide a generic data proxy to support the data transport between legacy systems and big data eco system.

### GDR and Eco System
![GDR and Eco System](https://github.com/yunjzhang/GDR/gdr_and_eco_system.png)

GDR support open source big data Eco system on multiple layers, it's a fundamental interface for all MPP process to read/write binary data from/to distribute file system.
and it also provide friendly tool to let user easily manipulate data among tradition file/DB systems and open source MPP systems.

### GDR infra
![GDR infra](https://github.com/yunjzhang/GDR/gdr_infra.png)

### GDR feature matrix
feature group | feature\data type | String | Integer/long | float/double | decimal | date | timestamp | bytes
------------- | ----------------- | ------ | ------------ | ------------ | ------- | ---- | --------- | -----
serialize style | delimiter | Y | Y | Y | Y | Y | Y | NA
X | dynamic length | Y | Y | Y | Y | Y | Y | Y
X | fix length | Y | Y | Y | Y | Y | Y | Y
null handle | nullable | Y | Y | Y | Y | Y | Y | Y
X | null bit flag | Y | Y | Y | Y | Y | Y | Y
X | nvl* | Y | Y | Y | Y | Y | Y | Y
fix value | | Y | Y | Y | Y | Y | Y | Y
endian | | NA | Y | Y | NA | NA | NA | Y
charset** | | Y | NA | NA | NA | NA | NA | NA
string | | format | NA | NA | NA | NA | Y | Y | NA
*nvl stands for null value ↔ default value replacement during serde
**UTF8 and iso-88591-1 are supported

### Highlight
    680 staging tables are using GDR in Hercules production by Dec 2018.
    500+ staging tables are testing with GDR on pre-production by Dec 2018.
    split feature speeds up 10+GB binary file reading by 20 times faster even more.
    malfunction chars replacing ensures data sanity for down streams.

### Note
    Ab Initio VOID is treated as Sting
    Ab Initio nested Record/Vector are not supported
    Some Date format is not supported(e.g. "V", "T" are not support, )
    Time Zone is not support(e.g. "YYYY-MM-DD HH24:MI:SS.NNNNNN+zo:ne")
    Strict Type Definition check on DML file(e.g. "All String" will fail)
    timestamp supports to millisecond
    default time zone is set as GMT-7 as local
    default endian is "LITTLE ENDIAN"
    "Unicode string" is not support
    some special bytes sequence won't be correctly handle by UTF-8, e.g.  (byte)0xed,(byte)0xa0,(byte)0xbd
    if dml defines Decimal column with scale=0, it's treated as long in hive, or it's treated as double as before
    hive limits max precision of decimal data type to 38, accordingly GDR will force to use long for decimal without scale and double with scale
    enable customized compress codec in ddl with key words
    com.ebay.dss.gdr.compress.output=true, com.ebay.dss.gdr.compress.codec='org.apache.hadoop.io.compress.GzipCodec'