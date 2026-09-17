package com.inspection.server.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 巡检图片 HDFS 上传接口。
 *
 * 端点：POST /api/images/upload
 * 入参：multipart/form-data，字段名 file
 * 出参：{ ok, hdfsPath, size, contentType }
 *
 * HDFS 客户端走 FileSystem.get(...)，
 * 内部走 RPC 协议连到 namenode:9000。
 */
@RestController
@RequestMapping("/api/images")
public class ImageController {

    private static final Logger log = LoggerFactory.getLogger(ImageController.class);

    /** HDFS 根路径（docker-compose 里 fs.defaultFS=hdfs://namenode:9000）。 */
    private static final String HDFS_URI = "hdfs://localhost:9000";
    /** 上传目录：HDFS 里会作为 /inspection-images/ 出现。 */
    private static final String HDFS_BASE_DIR = "/inspection-images";
    /** 容器里跑 Hadoop 的默认用户；本地开发用 root。 */
    private static final String HDFS_USER = "root";

    /**
     * 接收前端上传的图片，写入 HDFS。
     */
    @PostMapping("/upload")
    public ResponseEntity<Map<String, Object>> upload(@RequestParam("file") MultipartFile file) {
        Map<String, Object> resp = new LinkedHashMap<>();

        if (file == null || file.isEmpty()) {
            resp.put("ok", false);
            resp.put("error", "文件为空");
            return ResponseEntity.badRequest().body(resp);
        }

        String original = file.getOriginalFilename();
        String ext = "";
        if (original != null && original.contains(".")) {
            ext = original.substring(original.lastIndexOf('.'));
        }

        // 用 UUID 防同名覆盖，同时把原扩展名带上
        String objectName = UUID.randomUUID().toString().replace("-", "") + ext;
        Path target = new Path(HDFS_BASE_DIR + "/" + objectName);

        Configuration conf = new Configuration();
        // 必要时可以覆盖 fs.defaultFS；这里直接用 URI 传进去
        // conf.set("fs.defaultFS", HDFS_URI);
        // Hadoop RPC 超时别太长，避免 HDFS 故障时把 HTTP 接口拖死
        conf.set("dfs.client.socket-timeout", "10000");

        try (FileSystem fs = FileSystem.get(new URI(HDFS_URI), conf, HDFS_USER);
             FSDataOutputStream out = fs.create(target, true);   // overwrite=true
             var in = file.getInputStream()) {

            // 如果根目录不存在则创建
            Path baseDir = new Path(HDFS_BASE_DIR);
            if (!fs.exists(baseDir)) {
                fs.mkdirs(baseDir);
            }

            byte[] buf = new byte[8 * 1024];
            long total = 0;
            int n;
            while ((n = in.read(buf)) > 0) {
                out.write(buf, 0, n);
                total += n;
            }
            out.hflush();

            String hdfsPath = target.toString();   // 例：hdfs://localhost:9000/inspection-images/abc.png
            log.info("[HDFS UPLOAD] {} -> {} ({} bytes)", original, hdfsPath, total);

            resp.put("ok", true);
            resp.put("hdfsPath", hdfsPath);
            resp.put("hdfsUri", HDFS_URI + HDFS_BASE_DIR + "/" + objectName);
            resp.put("size", total);
            resp.put("originalName", original);
            resp.put("objectName", objectName);
            resp.put("contentType", file.getContentType());
            return ResponseEntity.ok(resp);

        } catch (IOException | InterruptedException | java.net.URISyntaxException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("HDFS upload failed: {}", e.getMessage(), e);
            resp.put("ok", false);
            resp.put("error", "HDFS 上传失败: " + e.getMessage());
            return ResponseEntity.status(500).body(resp);
        }
    }
}
