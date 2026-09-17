package com.inspection.server.repository;

import com.inspection.server.entity.DeviceData;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * 设备遥测数据 MongoDB 仓储。
 *
 * 继承自 {@link MongoRepository}，开箱即用提供：
 *   - save / saveAll
 *   - findById / findAll / count / delete ...
 *   - 分页、排序等
 */
@Repository
public interface DeviceDataRepository extends MongoRepository<DeviceData, String> {

    /** 根据 deviceId 查询所有历史数据（按 deviceId 过滤）。 */
    List<DeviceData> findByDeviceId(String deviceId);

    /** 查询某台设备的最新一条。 */
    Optional<DeviceData> findFirstByDeviceIdOrderByIdDesc(String deviceId);

    /** 按 deviceType 过滤。 */
    List<DeviceData> findByDeviceType(String deviceType);
}
