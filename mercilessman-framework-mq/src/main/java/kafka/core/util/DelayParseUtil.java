package kafka.core.util;


import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DelayParseUtil {
    public DelayParseUtil() {
    }

    public static List<Long> parseDelay(String delayOption) {
        if (StringUtils.isEmpty(delayOption)) {
            return Collections.emptyList();
        } else {
            String[] delays = delayOption.split(",");
            List<Long> delayList = new ArrayList(8);
            String[] arr = delays;
            int len$ = delays.length;

            for (int i = 0; i < len$; ++i) {
                String delayString = arr[i];
                delayList.add(Long.parseLong(delayString.trim()));
            }

            return delayList;
        }
    }
}

