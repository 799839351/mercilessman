package com.txk.idworker.simple;

import java.text.SimpleDateFormat;
import java.util.Date;

import static com.txk.idworker.simple.SnowflakeIdSimpleGenerator.*;

public class UidParseUtil {

    /**
     * @param  uid
     * @return 反解析UID
     */
    public static String parseUID(Long uid) {
        // 总位数
        long totalBits = 64L;
        // 标识
        long signBits = 1L;
        // 时间戳
        long timestampBits = 41L;
        // 解析Uid：标识 -- 时间戳 -- 数据中心 -- 机器码 --序列
        long sequence = (uid << (totalBits - SEQUENCE_BITS)) >>> (totalBits - SEQUENCE_BITS);
        long dataCenterId = (uid << (timestampBits + signBits)) >>> (totalBits - DATACENTER_ID_BITS);
        long workerId = (uid << (timestampBits + signBits + DATACENTER_ID_BITS)) >>> (totalBits - WORKER_ID_BITS);
        long deltaSeconds = uid >>> (DATACENTER_ID_BITS + WORKER_ID_BITS + SEQUENCE_BITS);
        // 时间处理(补上开始时间戳)
        Date thatTime = new Date(TWEPOCH + deltaSeconds);
        String date = new SimpleDateFormat(DATE_PATTERN_DEFAULT).format(thatTime);
        // 格式化输出
        return String.format(MSG_UID_PARSE, uid, date, workerId, dataCenterId, sequence);
    }
}

