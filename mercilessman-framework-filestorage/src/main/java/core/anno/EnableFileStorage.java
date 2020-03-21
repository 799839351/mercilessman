package core.anno;


import core.FileStorageSelector;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import({FileStorageSelector.class})
public @interface EnableFileStorage {
}