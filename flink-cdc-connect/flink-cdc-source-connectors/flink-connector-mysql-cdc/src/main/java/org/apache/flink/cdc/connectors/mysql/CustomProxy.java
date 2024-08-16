package org.apache.flink.cdc.connectors.mysql;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.function.Function;

public class CustomProxy<T> implements MethodInterceptor, Serializable {

    private T proxied;

    public CustomProxy(T proxied) {
        this.proxied = proxied;
    }

    public T getProxy(Function<T, T> func){
        //cglib 中增强器，用来创建动态代理
        Enhancer enhancer = new Enhancer();
        //设置要创建动态代理的类
        enhancer.setSuperclass(proxied.getClass());
        //设置回调，这里相当于是对于代理类上所有方法的调用，都会调用CallBack，而Callback则需要实现intercept()方法进行拦截
        enhancer.setCallback(this);
        //创建代理类
        T o = (T)enhancer.create();
        return func.apply(o);
    }

    @Override
    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
        return interceptAll(obj, method, args, proxy);
    }

    public static Object interceptAll(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
        StringBuilder s = new StringBuilder(Thread.currentThread().getName() + " -@@- " + method.getDeclaringClass().getName() + " 调用方法：" + method.getName());
        s.append("\n参数列表如下:");
        Type[] types = method.getGenericParameterTypes();
        for (int i = 0; i < args.length; i++) {
            String fromat = "类型: %s, 值: %s\n";
            String typeName = types[i].getTypeName();
            String value = null;
            if (args[i] == null) {
                value = "null";
            } else {
                value = args[i].toString();
            }
            s.append(String.format(fromat, typeName, value));
        }
//        System.out.println(s);
        return proxy.invokeSuper(obj, args);
    }

}
