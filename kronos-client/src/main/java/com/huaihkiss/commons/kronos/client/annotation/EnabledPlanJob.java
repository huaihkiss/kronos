package com.huaihkiss.commons.kronos.client.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface EnabledPlanJob {
    public String id() ;
    public boolean isSync() default true;
    public long executeTime() default 1000L;

}
