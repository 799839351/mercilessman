package core.model;

import java.lang.reflect.Field;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

public  interface BaseEnum {
    String DEFAULT_CODE_NAME = "code";
    String DEFAULT_DESC_NAME = "desc";

    default String getCode() {
        return this.getFieldValue("code");
    }

    default String getDesc() {
        return this.getFieldValue("desc");
    }

    static <T extends Enum<T>> T valueOfEnum(Class<T> enumClazz, String code) {
        Objects.requireNonNull(enumClazz);
        if (StringUtils.isBlank(code)) {
            throw new IllegalArgumentException("enum code can not be null");
        } else if (!enumClazz.isAssignableFrom(BaseEnum.class) && BaseEnum.class.isAssignableFrom(enumClazz)) {
            Enum[] enumConstantList = enumClazz.getEnumConstants();
            if (null != enumConstantList && enumConstantList.length != 0) {
                Enum[] enumList = enumConstantList;
                int var4 = enumConstantList.length;

                for(int i = 0; i < var4; ++i) {
                    T element = (T)enumList[i];
                    BaseEnum baseEnum = (BaseEnum)element;
                    if (code.equals(baseEnum.getCode())) {
                        return element;
                    }
                }

                return null;
            } else {
                throw new IllegalArgumentException("parse enum err");
            }
        } else {
            throw new IllegalArgumentException("enum must impl BaseEnum");
        }
    }



    default String getFieldValue(String defaultDescName) {
        try {
            Field field = this.getClass().getDeclaredField(defaultDescName);
            if (null == field) {
                return null;
            } else {
                field.setAccessible(true);
                return field.get(this).toString();
            }
        } catch (Exception var3) {
            throw new RuntimeException(var3);
        }
    }
}
