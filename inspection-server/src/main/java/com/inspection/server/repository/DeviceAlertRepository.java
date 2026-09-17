package com.inspection.server.repository;

import com.inspection.server.entity.DeviceAlert;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * 设备告警 ES 仓储。
 *
 * Spring Data Elasticsearch 会基于方法名自动派生查询：
 *   - findByAlertMessageContaining 会生成一个 match 查询。
 *
 * ES 8.x 客户端默认 strict mode，
 * 所以这里显式给出 {@code @Param} 没用上，但保留扩展性。
 */
@Repository
public interface DeviceAlertRepository extends ElasticsearchRepository<DeviceAlert, String> {

    /**
     * 模糊匹配告警消息内容（大小写不敏感的分词匹配）。
     * 等价 ES 查询：{"match": {"alertMessage": "<keyword>"}}
     *
     * @param keyword 关键字
     * @return 命中的告警列表
     */
    List<DeviceAlert> findByAlertMessageContaining(String keyword);
}
