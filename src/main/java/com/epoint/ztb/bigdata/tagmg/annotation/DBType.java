package com.epoint.ztb.bigdata.tagmg.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME) // 在运行时可以获取
@Target({ElementType.TYPE }) // 作用到类，方法，接口上等
@Inherited // 子类会继承
public @interface DBType
{
    public String value() default "";
}
