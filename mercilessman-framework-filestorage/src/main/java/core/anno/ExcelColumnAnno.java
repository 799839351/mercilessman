package core.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface ExcelColumnAnno {
    int index();

    String label();

    String format() default "yyyy-MM-dd HH:mm:ss";

    int size() default -1;
}
