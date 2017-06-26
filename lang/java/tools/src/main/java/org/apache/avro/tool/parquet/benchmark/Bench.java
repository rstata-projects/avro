package org.apache.avro.tool.parquet.benchmark;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class Bench {
  public static class ReceiptRecord {
    public String hashedIp;
    public String rawIp;
    public String eventTimestamp;
    public String eventDate;
    public String eventHour;
    public String host;
    public String path;
    public String referer;
    public String pxcelVar;
    public String dnt;
    public String userAgent;
    public String browser;
    public String browserVersion;
    public String os;
    public String osVersion;
    public String device;
    public String acceptHeader;
    public String acceptLanguageHeader;
    public String acceptEncodingHeader;
    public String countryCode;
    public String regionCode;
    public String latitude;
    public String longitude;
    public String dma;
    public String msa;
    public String timezone;
    public String areaCode;
    public String fips;
    public String city;
    public String zip;
    public String network;
    public String networkType;
    public String throughput;
    public String tagType;
    public String cls;
    public String eventId;
    public String uu;
    public String suu;
    public String puu;
    public String domain;
    public String redirectDomain;
    public String version;

    public void loadAvroSource(Decoder d) throws IOException {
      if (d.readIndex() != 0) {
        d.readNull();
        this.hashedIp = "";
      } else {
        this.hashedIp = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.rawIp = "";
      } else {
        this.rawIp = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.eventTimestamp = "";
      } else {
        this.eventTimestamp = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.eventDate = "";
      } else {
        this.eventDate = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.eventHour = "";
      } else {
        this.eventHour = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.host = "";
      } else {
        this.host = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.path = "";
      } else {
        this.path = d.readString();
      }
      skipStringMap(d);
      if (d.readIndex() != 0) {
        d.readNull();
        this.referer = "";
      } else {
        this.referer = d.readString();
      }
      skipStringMap(d);
      if (d.readIndex() != 0) {
        d.readNull();
        this.pxcelVar = "";
      } else {
        this.pxcelVar = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.dnt = "";
      } else {
        this.dnt = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.userAgent = "";
      } else {
        this.userAgent = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.browser = "";
      } else {
        this.browser = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.browserVersion = "";
      } else {
        this.browserVersion = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.os = "";
      } else {
        this.os = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.osVersion = "";
      } else {
        this.osVersion = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
      } else {
        this.device = "";
        this.device = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.acceptHeader = "";
      } else {
        this.acceptHeader = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.acceptLanguageHeader = "";
      } else {
        this.acceptLanguageHeader = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.acceptEncodingHeader = "";
      } else {
        this.acceptEncodingHeader = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.countryCode = "";
      } else {
        this.countryCode = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.regionCode = "";
      } else {
        this.regionCode = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.latitude = "";
      } else {
        this.latitude = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.longitude = "";
      } else {
        this.longitude = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.dma = "";
      } else {
        this.dma = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.msa = "";
      } else {
        this.msa = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.timezone = "";
      } else {
        this.timezone = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.areaCode = "";
      } else {
        this.areaCode = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.fips = "";
      } else {
        this.fips = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.city = "";
      } else {
        this.city = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.zip = "";
      } else {
        this.zip = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.network = "";
      } else {
        this.network = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.networkType = "";
      } else {
        this.networkType = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.throughput = "";
      } else {
        this.throughput = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.tagType = "";
      } else {
        this.tagType = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
      } else {
        this.cls = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.eventId = "";
      } else {
        this.eventId = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.uu = "";
      } else {
        this.uu = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.suu = "";
      } else {
        this.suu = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.puu = "";
      } else {
        this.puu = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.domain = "";
      } else {
        this.domain = d.readString();
      }
      if (d.readIndex() != 0) {
        this.redirectDomain = "";
        d.readNull();
      } else {
        this.redirectDomain = d.readString();
      }
      if (d.readIndex() != 0) {
        d.readNull();
      } else {
        for (long n = d.readArrayStart(); n != 0; n = d.arrayNext()) {
          for (long i = 0; i < n; i++) {
            if (d.readIndex() != 0) {
              d.readNull();
            } else {
              d.readInt();
            }
            d.readLong();
            if (d.readIndex() != 0) {
              d.readNull();
            } else {
              d.skipString();
            }
            if (d.readIndex() != 0) {
              d.readNull();
            } else {
              d.skipString();
            }
          }
        }
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.version = "";
      } else {
        this.version = d.readString();
      }
    }

    private void skipStringMap(Decoder d) throws IOException {
      if (d.readIndex() != 0) {
        d.readNull();
      } else {
        for (long n = d.readMapStart(); n != 0; n = d.mapNext()) {
          for (long i = 0; i < n; i++) {
            d.skipString();
            if (d.readIndex() != 0) {
              d.readNull();
            } else {
              d.skipString();
            }
          }
        }
      }
    }

    public static final String AVRO_SCHEMA =
      "{" +
        "  \"type\" : \"record\"," +
        "  \"name\" : \"topLevelRecord\"," +
        "  \"fields\" : [ {" +
        "    \"name\" : \"hashed_ip\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"raw_ip\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"event_timestamp\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"event_date\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"event_hour\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"host\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"path\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
                /*
                "    \"name\" : \"query\"," +
                "    \"type\" : [ {" +
                "      \"type\" : \"map\"," +
                "      \"values\" : [ \"string\", \"null\" ]" +
                "    }, \"null\" ]" +
                "  }, {" +
                */
        "    \"name\" : \"referer\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
                /*
                "    \"name\" : \"cookie\"," +
                "    \"type\" : [ {" +
                "      \"type\" : \"map\"," +
                "      \"values\" : [ \"string\", \"null\" ]" +
                "    }, \"null\" ]" +
                "  }, {" +
                */
        "    \"name\" : \"pxcel_var\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"dnt\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"user_agent\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"browser\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"browser_version\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"os\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"os_version\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"device\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"accept_header\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"accept_language_header\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"accept_encoding_header\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"country_code\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"region_code\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"latitude\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"longitude\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"dma\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"msa\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"timezone\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"area_code\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"fips\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"city\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"zip\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"network\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"network_type\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"throughput\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"tag_type\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"class\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"event_id\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"uu\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"suu\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"puu\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"domain\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
        "    \"name\" : \"redirect_domain\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  }, {" +
                /*
                "    \"name\" : \"partner_urls\"," +
                "    \"type\" : [ {" +
                "      \"type\" : \"array\"," +
                "      \"items\" : {" +
                "        \"type\" : \"record\"," +
                "        \"name\" : \"partner_url\"," +
                "        \"fields\" : [ {" +
                "          \"name\" : \"status\"," +
                "          \"type\" : [ {" +
                "            \"type\" : \"enum\"," +
                "            \"name\" : \"status\"," +
                "            \"symbols\" : [ \"failure\", \"success\", \"timeout\" ]" +
                "          }, \"null\" ]" +
                "        }, {" +
                "          \"name\" : \"latency_ms\"," +
                "          \"type\" : \"long\"" +
                "        }, {" +
                "          \"name\" : \"domain\"," +
                "          \"type\" : [ \"string\", \"null\" ]" +
                "        }, {" +
                "          \"name\" : \"beacon_id\"," +
                "          \"type\" : [ \"string\", \"null\" ]" +
                "        } ]" +
                "      }" +
                "    }, \"null\" ]" +
                "  }, {" +
                */
        "    \"name\" : \"version\"," +
        "    \"type\" : [ \"string\", \"null\" ]" +
        "  } ]" +
        "}";
    private static Schema avroSchemaObj = null;
    public static Schema getAvroSchema() {
      if (avroSchemaObj == null) {
        synchronized (Bench.class) {
          if (avroSchemaObj == null) {
            avroSchemaObj = new Schema.Parser().parse(AVRO_SCHEMA);
          }
        }
      }
      return avroSchemaObj;
    }

    public void loadAvro(Decoder d) throws IOException {
      this.hashedIp = d.readString();
      this.rawIp = d.readString();
      this.eventTimestamp = d.readString();
      this.eventDate = d.readString();
      this.eventHour = d.readString();
      this.host = d.readString();
      this.path = d.readString();
      this.referer = d.readString();
      this.pxcelVar = d.readString();
      this.dnt = d.readString();
      this.userAgent = d.readString();
      this.browser = d.readString();
      this.browserVersion = d.readString();
      this.os = d.readString();
      this.osVersion = d.readString();
      this.device = d.readString();
      this.acceptHeader = d.readString();
      this.acceptLanguageHeader = d.readString();
      this.acceptEncodingHeader = d.readString();
      this.countryCode = d.readString();
      this.regionCode = d.readString();
      this.latitude = d.readString();
      this.longitude = d.readString();
      this.dma = d.readString();
      this.msa = d.readString();
      this.timezone = d.readString();
      this.areaCode = d.readString();
      this.fips = d.readString();
      this.city = d.readString();
      this.zip = d.readString();
      this.network = d.readString();
      this.networkType = d.readString();
      this.throughput = d.readString();
      this.tagType = d.readString();
      this.cls = d.readString();
      this.eventId = d.readString();
      this.uu = d.readString();
      this.suu = d.readString();
      this.puu = d.readString();
      this.domain = d.readString();
      this.redirectDomain = d.readString();
      this.version = d.readString();
    }

    public void saveAvro(Encoder e) throws IOException {
      e.writeString(this.hashedIp);
      e.writeString(this.rawIp);
      e.writeString(this.eventTimestamp);
      e.writeString(this.eventDate);
      e.writeString(this.eventHour);
      e.writeString(this.host);
      e.writeString(this.path);
      e.writeString(this.referer);
      e.writeString(this.pxcelVar);
      e.writeString(this.dnt);
      e.writeString(this.userAgent);
      e.writeString(this.browser);
      e.writeString(this.browserVersion);
      e.writeString(this.os);
      e.writeString(this.osVersion);
      e.writeString(this.device);
      e.writeString(this.acceptHeader);
      e.writeString(this.acceptLanguageHeader);
      e.writeString(this.acceptEncodingHeader);
      e.writeString(this.countryCode);
      e.writeString(this.regionCode);
      e.writeString(this.latitude);
      e.writeString(this.longitude);
      e.writeString(this.dma);
      e.writeString(this.msa);
      e.writeString(this.timezone);
      e.writeString(this.areaCode);
      e.writeString(this.fips);
      e.writeString(this.city);
      e.writeString(this.zip);
      e.writeString(this.network);
      e.writeString(this.networkType);
      e.writeString(this.throughput);
      e.writeString(this.tagType);
      e.writeString(this.cls);
      e.writeString(this.eventId);
      e.writeString(this.uu);
      e.writeString(this.suu);
      e.writeString(this.puu);
      e.writeString(this.domain);
      e.writeString(this.redirectDomain);
      e.writeString(this.version);
    }

    public static String parquetSchema =
      "message receipt_record {" +
        "    required binary hashed_ip (UTF8);" +
        "    required binary raw_ip (UTF8);" +
        "    required binary event_timestamp (UTF8);" +
        "    required binary event_date (UTF8);" +
        "    required binary event_hour (UTF8);" +
        "    required binary host (UTF8);" +
        "    required binary path (UTF8);" +
        "    required binary referer (UTF8);" +
        "    required binary pxcel_var (UTF8);" +
        "    required binary dnt (UTF8);" +
        "    required binary user_agent (UTF8);" +
        "    required binary browser (UTF8);" +
        "    required binary browser_version (UTF8);" +
        "    required binary os (UTF8);" +
        "    required binary os_version (UTF8);" +
        "    required binary device (UTF8);" +
        "    required binary accept_header (UTF8);" +
        "    required binary accept_language_header (UTF8);" +
        "    required binary accept_encoding_header (UTF8);" +
        "    required binary country_code (UTF8);" +
        "    required binary region_code (UTF8);" +
        "    required binary latitude (UTF8);" +
        "    required binary longitude (UTF8);" +
        "    required binary dma (UTF8);" +
        "    required binary msa (UTF8);" +
        "    required binary timezone (UTF8);" +
        "    required binary area_code (UTF8);" +
        "    required binary fips (UTF8);" +
        "    required binary city (UTF8);" +
        "    required binary zip (UTF8);" +
        "    required binary network (UTF8);" +
        "    required binary network_type (UTF8);" +
        "    required binary throughput (UTF8);" +
        "    required binary tag_type (UTF8);" +
        "    required binary class (UTF8);" +
        "    required binary event_id (UTF8);" +
        "    required binary uu (UTF8);" +
        "    required binary suu (UTF8);" +
        "    required binary puu (UTF8);" +
        "    required binary domain (UTF8);" +
        "    required binary redirect_domain (UTF8);" +
        "    required binary version (UTF8);" +
        "}" +
        "";
  }

  public static class ReceiptRecordReader implements DatumReader<ReceiptRecord> {
    @Override
    public void setSchema(Schema schema) {
    }

    @Override
    public ReceiptRecord read(ReceiptRecord reuse, Decoder d) throws IOException {
      if (reuse == null) {
        reuse = new ReceiptRecord();
      }
      reuse.loadAvroSource(d);
      return reuse;
    }
  }

  public static class ReceiptRecordWriter implements DatumWriter<ReceiptRecord> {
    @Override
    public void setSchema(Schema schema) {
    }

    @Override
    public void write(ReceiptRecord datum, Encoder e) throws IOException {
      datum.saveAvro(e);
    }
  }

  public static class ReceiptRecordWriteSupport extends org.apache.parquet.hadoop.api.WriteSupport<ReceiptRecord> {
    private final org.apache.parquet.schema.MessageType schema;
    private org.apache.parquet.io.api.RecordConsumer consumer = null;

    public ReceiptRecordWriteSupport(org.apache.parquet.schema.MessageType schema) {
      this.schema = schema;
    }

    @Override
    public org.apache.parquet.hadoop.api.WriteSupport.WriteContext init(org.apache.hadoop.conf.Configuration conf) {
      return new org.apache.parquet.hadoop.api.WriteSupport.WriteContext(this.schema, new HashMap<String, String>());
    }

    @Override
    public void prepareForWrite(org.apache.parquet.io.api.RecordConsumer consumer) {
      this.consumer = consumer;
    }

    @Override
    public void write(ReceiptRecord datum) {
      this.consumer.startMessage();
      this.consumer.startField("hashed_ip", 0);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.hashedIp));
      this.consumer.endField("hashed_ip", 0);
      this.consumer.startField("raw_ip", 1);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.rawIp));
      this.consumer.endField("raw_ip", 1);
      this.consumer.startField("event_timestamp", 2);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.eventTimestamp));
      this.consumer.endField("event_timestamp", 2);
      this.consumer.startField("event_date", 3);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.eventDate));
      this.consumer.endField("event_date", 3);
      this.consumer.startField("event_hour", 4);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.eventHour));
      this.consumer.endField("event_hour", 4);
      this.consumer.startField("host", 5);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.host));
      this.consumer.endField("host", 5);
      this.consumer.startField("path", 6);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.path));
      this.consumer.endField("path", 6);
      this.consumer.startField("referer", 7);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.referer));
      this.consumer.endField("referer", 7);
      this.consumer.startField("pxcel_var", 8);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.pxcelVar));
      this.consumer.endField("pxcel_var", 8);
      this.consumer.startField("dnt", 9);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.dnt));
      this.consumer.endField("dnt", 9);
      this.consumer.startField("user_agent", 10);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.userAgent));
      this.consumer.endField("user_agent", 10);
      this.consumer.startField("browser", 11);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.browser));
      this.consumer.endField("browser", 11);
      this.consumer.startField("browser_version", 12);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.browserVersion));
      this.consumer.endField("browser_version", 12);
      this.consumer.startField("os", 13);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.os));
      this.consumer.endField("os", 13);
      this.consumer.startField("os_version", 14);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.osVersion));
      this.consumer.endField("os_version", 14);
      this.consumer.startField("device", 15);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.device));
      this.consumer.endField("device", 15);
      this.consumer.startField("accept_header", 16);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.acceptHeader));
      this.consumer.endField("accept_header", 16);
      this.consumer.startField("accept_language_header", 17);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.acceptLanguageHeader));
      this.consumer.endField("accept_language_header", 17);
      this.consumer.startField("accept_encoding_header", 18);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.acceptEncodingHeader));
      this.consumer.endField("accept_encoding_header", 18);
      this.consumer.startField("country_code", 19);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.countryCode));
      this.consumer.endField("country_code", 19);
      this.consumer.startField("region_code", 20);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.regionCode));
      this.consumer.endField("region_code", 20);
      this.consumer.startField("latitude", 21);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.latitude));
      this.consumer.endField("latitude", 21);
      this.consumer.startField("longitude", 22);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.longitude));
      this.consumer.endField("longitude", 22);
      this.consumer.startField("dma", 23);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.dma));
      this.consumer.endField("dma", 23);
      this.consumer.startField("msa", 24);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.msa));
      this.consumer.endField("msa", 24);
      this.consumer.startField("timezone", 25);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.timezone));
      this.consumer.endField("timezone", 25);
      this.consumer.startField("area_code", 26);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.areaCode));
      this.consumer.endField("area_code", 26);
      this.consumer.startField("fips", 27);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.fips));
      this.consumer.endField("fips", 27);
      this.consumer.startField("city", 28);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.city));
      this.consumer.endField("city", 28);
      this.consumer.startField("zip", 29);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.zip));
      this.consumer.endField("zip", 29);
      this.consumer.startField("network", 30);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.network));
      this.consumer.endField("network", 30);
      this.consumer.startField("network_type", 31);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.networkType));
      this.consumer.endField("network_type", 31);
      this.consumer.startField("throughput", 32);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.throughput));
      this.consumer.endField("throughput", 32);
      this.consumer.startField("tag_type", 33);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.tagType));
      this.consumer.endField("tag_type", 33);
      this.consumer.startField("class", 34);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.cls));
      this.consumer.endField("class", 34);
      this.consumer.startField("event_id", 35);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.eventId));
      this.consumer.endField("event_id", 35);
      this.consumer.startField("uu", 36);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.uu));
      this.consumer.endField("uu", 36);
      this.consumer.startField("suu", 37);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.suu));
      this.consumer.endField("suu", 37);
      this.consumer.startField("puu", 38);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.puu));
      this.consumer.endField("puu", 38);
      this.consumer.startField("domain", 39);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.domain));
      this.consumer.endField("domain", 39);
      this.consumer.startField("redirect_domain", 40);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.redirectDomain));
      this.consumer.endField("redirect_domain", 40);
      this.consumer.startField("version", 41);
      this.consumer.addBinary(org.apache.parquet.io.api.Binary.fromString(datum.version));
      this.consumer.endField("version", 41);
      this.consumer.endMessage();
    }
  }

  public enum Type {
    READ,
    AVRO,
    PARQUET_NATIVE
  }

  public static void benchmark(Type type, String src, String dst, int repeat) throws IOException {
    for (int i = 0; i < repeat; i++) {
      long start = System.currentTimeMillis();
      doBenchmark(type, src, dst);
      System.out.println("Elapsed time: " + (System.currentTimeMillis() - start) + " ms.");
    }
  }

  private static void doBenchmark(Type type, String src, String dst) throws IOException {
    DataFileReader<ReceiptRecord> df =
      new DataFileReader<ReceiptRecord>(new File(src), new ReceiptRecordReader());
    String dest = dst;
    switch (type) {
      case READ: {
        int n = 0;
        while (df.hasNext()) {
          ReceiptRecord row = df.next();
          n++;
        }
        System.out.println(n + " rows");
      }
      break;
      case AVRO: {
        DataFileWriter<ReceiptRecord> dfw =
          new DataFileWriter<ReceiptRecord>(new ReceiptRecordWriter());
        dfw.setCodec(CodecFactory.deflateCodec(5));
        dfw.setSyncInterval(64 * 1024);
        dfw.create(ReceiptRecord.getAvroSchema(), new File(dest));
        int n = 0;
        while (df.hasNext()) {
          ReceiptRecord row = df.next();
          dfw.append(row);
          n++;
        }
        dfw.flush();
        dfw.close();
        System.out.println(n + " rows");
      }
      break;
      case PARQUET_NATIVE: {
        Path path = new Path(dest);
        class WriterBuilder extends ParquetWriter.Builder<ReceiptRecord, WriterBuilder> {

          WriterBuilder(Path path) {
            super(path);
          }

          @Override
          protected WriterBuilder self() {
            return this;
          }

          @Override
          protected WriteSupport<ReceiptRecord> getWriteSupport(Configuration conf) {
            MessageType mt = MessageTypeParser.parseMessageType(ReceiptRecord.parquetSchema);
            return new ReceiptRecordWriteSupport(mt);
          }
        }
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        path.getFileSystem(conf).delete(path, true);
        ParquetWriter<ReceiptRecord> w = new WriterBuilder(path).withCompressionCodec(CompressionCodecName.GZIP)
          .withConf(conf)
          .withDictionaryEncoding(false).build();

        int n = 0;
        while (df.hasNext()) {
          ReceiptRecord row = df.next();
          w.write(row);
          n++;
        }
        w.close();
        System.out.println(n + " rows");
      }
    }
    df.close();
  }
}
