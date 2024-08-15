package org.apache.flink.cdc.connectors.mysql;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlSourceReaderContext;
import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlSplitReader;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.function.Function;
import java.util.function.Supplier;

public class MySqlSourceReaderProxy<MySqlSourceReader> implements MethodInterceptor, Serializable {

    private MySqlSourceReader proxied;

    public MySqlSourceReaderProxy(MySqlSourceReader proxied) {
        this.proxied = proxied;
    }

    public MySqlSourceReader getProxy(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecords>> elementQueue,
            Supplier<MySqlSplitReader> splitReaderSupplier,
            RecordEmitter recordEmitter,
            Configuration config,
            MySqlSourceReaderContext context,
            MySqlSourceConfig sourceConfig
    ){
        //cglib 中增强器，用来创建动态代理
        Enhancer enhancer = new Enhancer();
        //设置要创建动态代理的类
        enhancer.setSuperclass(proxied.getClass());
        //设置回调，这里相当于是对于代理类上所有方法的调用，都会调用CallBack，而Callback则需要实现intercept()方法进行拦截
        enhancer.setCallback(this);
        //创建代理类
        MySqlSourceReader o = (MySqlSourceReader)enhancer.create(
                new Class[]{FutureCompletingBlockingQueue.class, Supplier.class, RecordEmitter.class, Configuration.class, MySqlSourceReaderContext.class, MySqlSourceConfig.class},
                new Object[]{elementQueue, splitReaderSupplier, recordEmitter, config, context, sourceConfig}
        );
        return o;
    }

    @Override
    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
        return CustomProxy.interceptAll(obj, method, args, proxy);
    }

}
