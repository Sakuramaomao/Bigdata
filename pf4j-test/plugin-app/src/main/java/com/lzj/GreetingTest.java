package com.lzj;

import org.pf4j.JarPluginManager;
import org.pf4j.PluginManager;

import java.nio.file.Paths;
import java.util.List;

/**
 * Hello world!
 */
public class GreetingTest {
    private static final String PLUGIN_ID = "greeting-plugin";
    private static final String JAR_PATH = "D:/plugins-1.0-SNAPSHOT.jar";

    public static void main(String[] args) throws IllegalAccessException, InstantiationException {
        // jar管理器。
        PluginManager jarPluginManager = new JarPluginManager();
        // 加载指定位置jar文件。nio
        jarPluginManager.loadPlugin(Paths.get(JAR_PATH));
        // 启动指定插件。
        jarPluginManager.startPlugin(PLUGIN_ID);

        // 调用插件中所有接口实现类中的方法。
        List<Greeting> greetings = jarPluginManager.getExtensions(Greeting.class);
        List<Class<?>> extensionClasses1 = jarPluginManager.getExtensionClasses(PLUGIN_ID);
        List<Class<? extends Greeting>> extensionClasses = jarPluginManager.getExtensionClasses(Greeting.class);

        // 运行指定实现类名称中的方法。
        for (Class<?> klass : extensionClasses1) {
            if ("FirstGreetingImpl".equals(klass.getSimpleName())) {
                Greeting greeting = (Greeting) klass.newInstance();
                greeting.greeting();
                System.out.println(greeting);
            }
        }

        // 运行所有的实现类方法。
        //for (Greeting g : greetings) {
        //    System.out.println(g.greeting());
        //}

        // 停止并卸载指定插件。
        jarPluginManager.stopPlugin(PLUGIN_ID);
        jarPluginManager.unloadPlugin(PLUGIN_ID);
    }
}
