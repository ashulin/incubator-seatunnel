/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.file.ftp.source;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FilePluginException;
import org.apache.seatunnel.connectors.seatunnel.file.ftp.config.FtpConf;
import org.apache.seatunnel.connectors.seatunnel.file.ftp.config.FtpConfig;
import org.apache.seatunnel.connectors.seatunnel.file.source.BaseFileSource;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategyFactory;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.io.IOException;

@AutoService(SeaTunnelSource.class)
public class FtpFileSource extends BaseFileSource {
    @Override
    public String getPluginName() {
        return FileSystemType.FTP.getFileSystemPluginName();
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, FtpConfig.FILE_PATH, FtpConfig.FILE_TYPE,
                FtpConfig.FTP_HOST, FtpConfig.FTP_PORT,
                FtpConfig.FTP_USERNAME, FtpConfig.FTP_PASSWORD);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }
        FileFormat fileFormat = FileFormat.valueOf(pluginConfig.getString(FtpConfig.FILE_TYPE).toUpperCase());
        if (fileFormat == FileFormat.ORC || fileFormat == FileFormat.PARQUET) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, "Ftp file source connector only support read [text, csv, json] files");
        }
        readStrategy = ReadStrategyFactory.of(pluginConfig.getString(FtpConfig.FILE_TYPE));
        String path = pluginConfig.getString(FtpConfig.FILE_PATH);
        hadoopConf = FtpConf.buildWithConfig(pluginConfig);
        try {
            filePaths = readStrategy.getFileNamesByPath(hadoopConf, path);
        } catch (IOException e) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, "Check file path fail.");
        }
        // support user-defined schema
        // only json type support user-defined schema now
        if (pluginConfig.hasPath(SeaTunnelSchema.SCHEMA) && fileFormat.equals(FileFormat.JSON)) {
            Config schemaConfig = pluginConfig.getConfig(SeaTunnelSchema.SCHEMA);
            rowType = SeaTunnelSchema
                    .buildWithConfig(schemaConfig)
                    .getSeaTunnelRowType();
            readStrategy.setSeaTunnelRowTypeInfo(rowType);
        } else {
            try {
                rowType = readStrategy.getSeaTunnelRowTypeInfo(hadoopConf, filePaths.get(0));
            } catch (FilePluginException e) {
                throw new PrepareFailException(getPluginName(), PluginType.SOURCE, "Read file schema error.", e);
            }
        }
    }
}
