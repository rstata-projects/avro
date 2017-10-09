/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.tool.parquet.benchmark;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.tool.parquet.benchmark.Bench.Type;
import org.apache.avro.io.parquet.ParquetEncoder;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
// import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class Bench3 {
  public static class ReceiptRecord {
    public Utf8 hashedIp;
    public Utf8 rawIp;
    public Utf8 eventTimestamp;
    public Utf8 eventDate;
    public Utf8 eventHour;
    public Utf8 host;
    public Utf8 path;
    public Utf8 referer;
    public Utf8 pxcelVar;
    public Utf8 dnt;
    public Utf8 userAgent;
    public Utf8 browser;
    public Utf8 browserVersion;
    public Utf8 os;
    public Utf8 osVersion;
    public Utf8 device;
    public Utf8 acceptHeader;
    public Utf8 acceptLanguageHeader;
    public Utf8 acceptEncodingHeader;
    public Utf8 countryCode;
    public Utf8 regionCode;
    public Utf8 latitude;
    public Utf8 longitude;
    public Utf8 dma;
    public Utf8 msa;
    public Utf8 timezone;
    public Utf8 areaCode;
    public Utf8 fips;
    public Utf8 city;
    public Utf8 zip;
    public Utf8 network;
    public Utf8 networkType;
    public Utf8 throughput;
    public Utf8 tagType;
    public Utf8 cls;
    public Utf8 eventId;
    public Utf8 uu;
    public Utf8 suu;
    public Utf8 puu;
    public Utf8 domain;
    public Utf8 redirectDomain;
    public Utf8 version;

    public void loadAvroSource(Decoder d) throws IOException {
      if (d.readIndex() != 0) {
        d.readNull();
        this.hashedIp = new Utf8();
      } else {
        this.hashedIp = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.rawIp = new Utf8();
      } else {
        this.rawIp = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.eventTimestamp = new Utf8();
      } else {
        this.eventTimestamp = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.eventDate = new Utf8();
      } else {
        this.eventDate = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.eventHour = new Utf8();
      } else {
        this.eventHour = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.host = new Utf8();
      } else {
        this.host = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.path = new Utf8();
      } else {
        this.path = d.readString((Utf8)null);
      }
      skipStringMap(d);
      if (d.readIndex() != 0) {
        d.readNull();
        this.referer = new Utf8();
      } else {
        this.referer = d.readString((Utf8)null);
      }
      skipStringMap(d);
      if (d.readIndex() != 0) {
        d.readNull();
        this.pxcelVar = new Utf8();
      } else {
        this.pxcelVar = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.dnt = new Utf8();
      } else {
        this.dnt = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.userAgent = new Utf8();
      } else {
        this.userAgent = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.browser = new Utf8();
      } else {
        this.browser = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.browserVersion = new Utf8();
      } else {
        this.browserVersion = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.os = new Utf8();
      } else {
        this.os = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.osVersion = new Utf8();
      } else {
        this.osVersion = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
      } else {
        this.device = new Utf8();
        this.device = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.acceptHeader = new Utf8();
      } else {
        this.acceptHeader = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.acceptLanguageHeader = new Utf8();
      } else {
        this.acceptLanguageHeader = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.acceptEncodingHeader = new Utf8();
      } else {
        this.acceptEncodingHeader = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.countryCode = new Utf8();
      } else {
        this.countryCode = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.regionCode = new Utf8();
      } else {
        this.regionCode = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.latitude = new Utf8();
      } else {
        this.latitude = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.longitude = new Utf8();
      } else {
        this.longitude = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.dma = new Utf8();
      } else {
        this.dma = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.msa = new Utf8();
      } else {
        this.msa = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.timezone = new Utf8();
      } else {
        this.timezone = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.areaCode = new Utf8();
      } else {
        this.areaCode = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.fips = new Utf8();
      } else {
        this.fips = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.city = new Utf8();
      } else {
        this.city = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.zip = new Utf8();
      } else {
        this.zip = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.network = new Utf8();
      } else {
        this.network = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.networkType = new Utf8();
      } else {
        this.networkType = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.throughput = new Utf8();
      } else {
        this.throughput = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.tagType = new Utf8();
      } else {
        this.tagType = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
      } else {
        this.cls = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.eventId = new Utf8();
      } else {
        this.eventId = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.uu = new Utf8();
      } else {
        this.uu = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.suu = new Utf8();
      } else {
        this.suu = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.puu = new Utf8();
      } else {
        this.puu = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        d.readNull();
        this.domain = new Utf8();
      } else {
        this.domain = d.readString((Utf8)null);
      }
      if (d.readIndex() != 0) {
        this.redirectDomain = new Utf8();
        d.readNull();
      } else {
        this.redirectDomain = d.readString((Utf8)null);
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
        this.version = new Utf8();
      } else {
        this.version = d.readString((Utf8)null);
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
      this.hashedIp = d.readString((Utf8)null);
      this.rawIp = d.readString((Utf8)null);
      this.eventTimestamp = d.readString((Utf8)null);
      this.eventDate = d.readString((Utf8)null);
      this.eventHour = d.readString((Utf8)null);
      this.host = d.readString((Utf8)null);
      this.path = d.readString((Utf8)null);
      this.referer = d.readString((Utf8)null);
      this.pxcelVar = d.readString((Utf8)null);
      this.dnt = d.readString((Utf8)null);
      this.userAgent = d.readString((Utf8)null);
      this.browser = d.readString((Utf8)null);
      this.browserVersion = d.readString((Utf8)null);
      this.os = d.readString((Utf8)null);
      this.osVersion = d.readString((Utf8)null);
      this.device = d.readString((Utf8)null);
      this.acceptHeader = d.readString((Utf8)null);
      this.acceptLanguageHeader = d.readString((Utf8)null);
      this.acceptEncodingHeader = d.readString((Utf8)null);
      this.countryCode = d.readString((Utf8)null);
      this.regionCode = d.readString((Utf8)null);
      this.latitude = d.readString((Utf8)null);
      this.longitude = d.readString((Utf8)null);
      this.dma = d.readString((Utf8)null);
      this.msa = d.readString((Utf8)null);
      this.timezone = d.readString((Utf8)null);
      this.areaCode = d.readString((Utf8)null);
      this.fips = d.readString((Utf8)null);
      this.city = d.readString((Utf8)null);
      this.zip = d.readString((Utf8)null);
      this.network = d.readString((Utf8)null);
      this.networkType = d.readString((Utf8)null);
      this.throughput = d.readString((Utf8)null);
      this.tagType = d.readString((Utf8)null);
      this.cls = d.readString((Utf8)null);
      this.eventId = d.readString((Utf8)null);
      this.uu = d.readString((Utf8)null);
      this.suu = d.readString((Utf8)null);
      this.puu = d.readString((Utf8)null);
      this.domain = d.readString((Utf8)null);
      this.redirectDomain = d.readString((Utf8)null);
      this.version = d.readString((Utf8)null);
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

    public static final String PARQUET_SCHEMA =
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
      this.consumer.addBinary(Binary.fromByteArray(datum.hashedIp.getBytes(), 0, datum.hashedIp.getByteLength()));
      this.consumer.endField("hashed_ip", 0);
      this.consumer.startField("raw_ip", 1);
      this.consumer.addBinary(Binary.fromByteArray(datum.rawIp.getBytes(), 0, datum.rawIp.getByteLength()));
      this.consumer.endField("raw_ip", 1);
      this.consumer.startField("event_timestamp", 2);
      this.consumer.addBinary(Binary.fromByteArray(datum.eventTimestamp.getBytes(), 0, datum.eventTimestamp.getByteLength()));
      this.consumer.endField("event_timestamp", 2);
      this.consumer.startField("event_date", 3);
      this.consumer.addBinary(Binary.fromByteArray(datum.eventDate.getBytes(), 0, datum.eventDate.getByteLength()));
      this.consumer.endField("event_date", 3);
      this.consumer.startField("event_hour", 4);
      this.consumer.addBinary(Binary.fromByteArray(datum.eventHour.getBytes(), 0, datum.eventHour.getByteLength()));
      this.consumer.endField("event_hour", 4);
      this.consumer.startField("host", 5);
      this.consumer.addBinary(Binary.fromByteArray(datum.host.getBytes(), 0, datum.host.getByteLength()));
      this.consumer.endField("host", 5);
      this.consumer.startField("path", 6);
      this.consumer.addBinary(Binary.fromByteArray(datum.path.getBytes(), 0, datum.path.getByteLength()));
      this.consumer.endField("path", 6);
      this.consumer.startField("referer", 7);
      this.consumer.addBinary(Binary.fromByteArray(datum.referer.getBytes(), 0, datum.referer.getByteLength()));
      this.consumer.endField("referer", 7);
      this.consumer.startField("pxcel_var", 8);
      this.consumer.addBinary(Binary.fromByteArray(datum.pxcelVar.getBytes(), 0, datum.pxcelVar.getByteLength()));
      this.consumer.endField("pxcel_var", 8);
      this.consumer.startField("dnt", 9);
      this.consumer.addBinary(Binary.fromByteArray(datum.dnt.getBytes(), 0, datum.dnt.getByteLength()));
      this.consumer.endField("dnt", 9);
      this.consumer.startField("user_agent", 10);
      this.consumer.addBinary(Binary.fromByteArray(datum.userAgent.getBytes(), 0, datum.userAgent.getByteLength()));
      this.consumer.endField("user_agent", 10);
      this.consumer.startField("browser", 11);
      this.consumer.addBinary(Binary.fromByteArray(datum.browser.getBytes(), 0, datum.browser.getByteLength()));
      this.consumer.endField("browser", 11);
      this.consumer.startField("browser_version", 12);
      this.consumer.addBinary(Binary.fromByteArray(datum.browserVersion.getBytes(), 0, datum.browserVersion.getByteLength()));
      this.consumer.endField("browser_version", 12);
      this.consumer.startField("os", 13);
      this.consumer.addBinary(Binary.fromByteArray(datum.os.getBytes(), 0, datum.os.getByteLength()));
      this.consumer.endField("os", 13);
      this.consumer.startField("os_version", 14);
      this.consumer.addBinary(Binary.fromByteArray(datum.osVersion.getBytes(), 0, datum.osVersion.getByteLength()));
      this.consumer.endField("os_version", 14);
      this.consumer.startField("device", 15);
      this.consumer.addBinary(Binary.fromByteArray(datum.device.getBytes(), 0, datum.device.getByteLength()));
      this.consumer.endField("device", 15);
      this.consumer.startField("accept_header", 16);
      this.consumer.addBinary(Binary.fromByteArray(datum.acceptHeader.getBytes(), 0, datum.acceptHeader.getByteLength()));
      this.consumer.endField("accept_header", 16);
      this.consumer.startField("accept_language_header", 17);
      this.consumer.addBinary(Binary.fromByteArray(datum.acceptLanguageHeader.getBytes(), 0, datum.acceptLanguageHeader.getByteLength()));
      this.consumer.endField("accept_language_header", 17);
      this.consumer.startField("accept_encoding_header", 18);
      this.consumer.addBinary(Binary.fromByteArray(datum.acceptEncodingHeader.getBytes(), 0, datum.acceptEncodingHeader.getByteLength()));
      this.consumer.endField("accept_encoding_header", 18);
      this.consumer.startField("country_code", 19);
      this.consumer.addBinary(Binary.fromByteArray(datum.countryCode.getBytes(), 0, datum.countryCode.getByteLength()));
      this.consumer.endField("country_code", 19);
      this.consumer.startField("region_code", 20);
      this.consumer.addBinary(Binary.fromByteArray(datum.regionCode.getBytes(), 0, datum.regionCode.getByteLength()));
      this.consumer.endField("region_code", 20);
      this.consumer.startField("latitude", 21);
      this.consumer.addBinary(Binary.fromByteArray(datum.latitude.getBytes(), 0, datum.latitude.getByteLength()));
      this.consumer.endField("latitude", 21);
      this.consumer.startField("longitude", 22);
      this.consumer.addBinary(Binary.fromByteArray(datum.longitude.getBytes(), 0, datum.longitude.getByteLength()));
      this.consumer.endField("longitude", 22);
      this.consumer.startField("dma", 23);
      this.consumer.addBinary(Binary.fromByteArray(datum.dma.getBytes(), 0, datum.dma.getByteLength()));
      this.consumer.endField("dma", 23);
      this.consumer.startField("msa", 24);
      this.consumer.addBinary(Binary.fromByteArray(datum.msa.getBytes(), 0, datum.msa.getByteLength()));
      this.consumer.endField("msa", 24);
      this.consumer.startField("timezone", 25);
      this.consumer.addBinary(Binary.fromByteArray(datum.timezone.getBytes(), 0, datum.timezone.getByteLength()));
      this.consumer.endField("timezone", 25);
      this.consumer.startField("area_code", 26);
      this.consumer.addBinary(Binary.fromByteArray(datum.areaCode.getBytes(), 0, datum.areaCode.getByteLength()));
      this.consumer.endField("area_code", 26);
      this.consumer.startField("fips", 27);
      this.consumer.addBinary(Binary.fromByteArray(datum.fips.getBytes(), 0, datum.fips.getByteLength()));
      this.consumer.endField("fips", 27);
      this.consumer.startField("city", 28);
      this.consumer.addBinary(Binary.fromByteArray(datum.city.getBytes(), 0, datum.city.getByteLength()));
      this.consumer.endField("city", 28);
      this.consumer.startField("zip", 29);
      this.consumer.addBinary(Binary.fromByteArray(datum.zip.getBytes(), 0, datum.zip.getByteLength()));
      this.consumer.endField("zip", 29);
      this.consumer.startField("network", 30);
      this.consumer.addBinary(Binary.fromByteArray(datum.network.getBytes(), 0, datum.network.getByteLength()));
      this.consumer.endField("network", 30);
      this.consumer.startField("network_type", 31);
      this.consumer.addBinary(Binary.fromByteArray(datum.networkType.getBytes(), 0, datum.networkType.getByteLength()));
      this.consumer.endField("network_type", 31);
      this.consumer.startField("throughput", 32);
      this.consumer.addBinary(Binary.fromByteArray(datum.throughput.getBytes(), 0, datum.throughput.getByteLength()));
      this.consumer.endField("throughput", 32);
      this.consumer.startField("tag_type", 33);
      this.consumer.addBinary(Binary.fromByteArray(datum.tagType.getBytes(), 0, datum.tagType.getByteLength()));
      this.consumer.endField("tag_type", 33);
      this.consumer.startField("class", 34);
      this.consumer.addBinary(Binary.fromByteArray(datum.cls.getBytes(), 0, datum.cls.getByteLength()));
      this.consumer.endField("class", 34);
      this.consumer.startField("event_id", 35);
      this.consumer.addBinary(Binary.fromByteArray(datum.eventId.getBytes(), 0, datum.eventId.getByteLength()));
      this.consumer.endField("event_id", 35);
      this.consumer.startField("uu", 36);
      this.consumer.addBinary(Binary.fromByteArray(datum.uu.getBytes(), 0, datum.uu.getByteLength()));
      this.consumer.endField("uu", 36);
      this.consumer.startField("suu", 37);
      this.consumer.addBinary(Binary.fromByteArray(datum.suu.getBytes(), 0, datum.suu.getByteLength()));
      this.consumer.endField("suu", 37);
      this.consumer.startField("puu", 38);
      this.consumer.addBinary(Binary.fromByteArray(datum.puu.getBytes(), 0, datum.puu.getByteLength()));
      this.consumer.endField("puu", 38);
      this.consumer.startField("domain", 39);
      this.consumer.addBinary(Binary.fromByteArray(datum.domain.getBytes(), 0, datum.domain.getByteLength()));
      this.consumer.endField("domain", 39);
      this.consumer.startField("redirect_domain", 40);
      this.consumer.addBinary(Binary.fromByteArray(datum.redirectDomain.getBytes(), 0, datum.redirectDomain.getByteLength()));
      this.consumer.endField("redirect_domain", 40);
      this.consumer.startField("version", 41);
      this.consumer.addBinary(Binary.fromByteArray(datum.version.getBytes(), 0, datum.version.getByteLength()));
      this.consumer.endField("version", 41);
      this.consumer.endMessage();
    }
  }

  public static void benchmark(Type type, String src, String dst, int repeat) throws IOException {
    for (int i = 0; i < repeat; i++) {
      doBenchmark(type, src, dst);
    }
  }

  private static void doBenchmark(Type type, String src, String dst) throws IOException {
    DataFileReader<ReceiptRecord> df =
      new DataFileReader<ReceiptRecord>(new File(src), new ReceiptRecordReader());
    ArrayList<ReceiptRecord> data = new ArrayList<ReceiptRecord>(10500);
    int n;
    for (n = 0; df.hasNext() && n < 10000; n++) {
      data.add(df.next());
    }
    df.close();

    String dest = dst;
    long start = System.currentTimeMillis();
    switch (type) {
      case READ: {
      }
      break;
      case AVRO: {
        DataFileWriter<ReceiptRecord> dfw =
          new DataFileWriter<ReceiptRecord>(new ReceiptRecordWriter());
        dfw.setCodec(CodecFactory.deflateCodec(5));
        dfw.setSyncInterval(64 * 1024);
        dfw.create(ReceiptRecord.getAvroSchema(), new File(dest));
        for (int k = 0; k < 1000; k++) {
          for (ReceiptRecord row: data) {
            dfw.append(row);
          }
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
            MessageType mt = MessageTypeParser.parseMessageType(ReceiptRecord.PARQUET_SCHEMA);
            return new ReceiptRecordWriteSupport(mt);
          }
        }
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        path.getFileSystem(conf).delete(path, true);
/*
        ParquetWriter<ReceiptRecord> w = new WriterBuilder(path).withCompressionCodec(CompressionCodecName.GZIP)
          .withConf(conf)
          .withDictionaryEncoding(false).build();
*/
        ParquetWriter<ReceiptRecord> w = new WriterBuilder(path)
          .withConf(conf).build();

        for (int k = 0; k < 1000; k++) {
          for (ReceiptRecord row: data) {
            w.write(row);
          }
        }
        w.close();
        System.out.println(n + " rows");
        break;
      }
/*      case PARQUET_AVRO: { */
    default: {
        Path path = new Path(dest);
        MessageType mt = MessageTypeParser.parseMessageType(ReceiptRecord.PARQUET_SCHEMA);
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        path.getFileSystem(conf).delete(path, true);
        ParquetEncoder pe = new ParquetEncoder(path, mt, ParquetProperties.builder().build());
        for (int k = 0; k < 1000; k++) {
          for (ReceiptRecord row: data) {
            row.saveAvro(pe);
          }
        }
        pe.flush();
        pe.close();
        System.out.println(n + " rows");
        break;
      }
    }
    System.out.println("Elapsed time: " + (System.currentTimeMillis() - start) + " ms.");
  }
}
