DataStream API是Flink的核心层API。一个Flink程序，其实就是对DataStream的各种转换。具体来说，代码基本上都由以下几部分构成：
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698412364021-b64005ad-f476-4c04-ad52-9c757c17e6d7.png#averageHue=%23f3ece8&clientId=u9a879084-7a0b-4&from=paste&id=uf779d704&originHeight=319&originWidth=1280&originalType=binary&ratio=2&rotation=0&showTitle=false&size=1227336&status=done&style=none&taskId=u4b602ebf-b09f-48cc-a7bc-c2ff3796dbe&title=)
# 5.1 执行环境（Execution Environment）
Flink程序可以在各种上下文环境中运行：我们可以在本地JVM中执行程序，也可以提交到远程集群上运行。
不同的环境，代码的提交运行的过程会有所不同。这就要求我们在提交作业执行计算时，首先必须获取当前Flink的运行环境，从而建立起与Flink框架之间的联系。
## 5.1.1 创建执行环境
我们要获取的执行环境，是`StreamExecutionEnvironment`类的对象，这是所有Flink程序的基础。在代码中创建执行环境的方式，就是调用这个类的静态方法，具体有以下三种。

1. `**getExecutionEnvironment**`

最简单的方式，就是直接调用`getExecutionEnvironment`方法。它会根据当前运行的上下文直接得到正确的结果：如果程序是独立运行的，就返回一个本地执行环境；如果是创建了jar包，然后从命令行调用它并提交到集群执行，那么就返回集群的执行环境。也就是说，这个方法会根据当前运行的方式，自行决定该返回什么样的运行环境。
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```
这种方式，用起来简单高效，是最常用的一种创建执行环境的方式。

2. `**createLocalEnvironment**`

这个方法返回一个本地执行环境。可以在调用时传入一个参数，指定默认的并行度；如果不传入，则默认并行度就是本地的CPU核心数。
```java
StreamExecutionEnvironment localEnv = StreamExecutionEnvironment.createLocalEnvironment();
```

3. `**createRemoteEnvironment**`

这个方法返回集群执行环境。需要在调用时指定`JobManager`的主机名和端口号，并指定要在集群中运行的Jar包。
```java
StreamExecutionEnvironment remoteEnv = StreamExecutionEnvironment
		.createRemoteEnvironment(
		"host",                   // JobManager主机名
		1234,                     // JobManager进程端口号
			"path/to/jarFile.jar"  // 提交给JobManager的JAR包
		); 
```
在获取到程序执行环境后，我们还可以对执行环境进行灵活的设置。比如可以全局设置程序的并行度、禁用算子链，还可以定义程序的时间语义、配置容错机制。
## 5.1.2 执行模式（Execution Mode）
从Flink 1.12开始，官方推荐的做法是直接使用`DataStream API`，在提交任务时通过将执行模式设为`BATCH`来进行批处理。不建议使用`DataSet API`。
```java
// 流处理环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```
`DataStream API`执行模式包括：流执行模式、批执行模式和自动模式。

1. **流执行模式（Streaming）**

这是DataStream API最经典的模式，一般用于需要持续实时处理的无界数据流。默认情况下，程序使用的就是Streaming执行模式。

2. **批执行模式（Batch）**

专门用于批处理的执行模式。

3. **自动模式（AutoMatic）**

在这种模式下，将由程序根据输入数据源是否有界，来自动选择执行模式。
批执行模式的使用。主要有两种方式：

- **通过命令行配置**
```java
bin/flink run -Dexecution.runtime-mode=BATCH ...
```
在提交作业时，增加execution.runtime-mode参数，指定值为BATCH。

- **通过代码配置**
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRuntimeMode(RuntimeExecutionMode.BATCH);
```
在代码中，直接基于执行环境调用`setRuntimeMode`方法，传入`BATCH`模式。
实际应用中一般不会在代码中配置，而是使用命令行，这样更加灵活。
## 5.1.3 触发程序执行
需要注意的是，写完输出（`sink`）操作并不代表程序已经结束。因为当`main()`方法被调用时，其实只是定义了作业的每个执行操作，然后添加到数据流图中；这时并没有真正处理数据——因为数据可能还没来。Flink是由事件驱动的，只有等到数据到来，才会触发真正的计算，这也被称为“延迟执行”或“懒执行”。
所以我们需要显式地调用执行环境的`execute()`方法，来触发程序执行。`execute()`方法将一直等待作业完成，然后返回一个执行结果（`JobExecutionResult`）。
```java
env.execute();
```
# 5.2 源算子（Source）
Flink可以从各种来源获取数据，然后构建`DataStream`进行转换处理。一般将数据的输入来源称为数据源（data source），而读取数据的算子就是源算子（source operator）。所以，`source`就是我们整个处理程序的输入端。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698416914671-900a3d95-6500-4e84-b43b-d74d9c4267e8.png#averageHue=%23ece4e0&clientId=uceacbb65-3c84-4&from=paste&id=u9cae811f&originHeight=149&originWidth=1280&originalType=binary&ratio=2&rotation=0&showTitle=false&size=573306&status=done&style=none&taskId=u71ab361d-b89f-42ee-a726-b75a0061d4a&title=)
在Flink1.12以前，旧的添加`source`的方式，是调用执行环境的`addSource()`方法：
```java
DataStream<String> stream = env.addSource(...);
```
方法传入的参数是一个“源函数”（source function），需要实现`SourceFunction`接口。
从Flink1.12开始，主要使用流批统一的新`Source`架构：
```java
DataStreamSource<String> stream = env.fromSource(…)
```
Flink直接提供了很多预实现的接口，此外还有很多外部连接工具也帮我们实现了对应的`Source`，通常情况下足以应对我们的实际需求。
## 5.2.1 准备工作
为了方便练习，这里使用`WaterSensor`作为数据模型。

| **字段名** | **数据类型** | **说明** |
| --- | --- | --- |
| **id** | String | 水位传感器类型 |
| **ts** | Long | 传感器记录时间戳 |
| **vc** | Integer | 水位记录 |

具体代码如下：
```java
public class WaterSensor {
    public String id;
    public Long ts;
    public Integer vc;

    public WaterSensor() {
    }

    public WaterSensor(String id, Long ts, Integer vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Integer getVc() {
        return vc;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WaterSensor that = (WaterSensor) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(ts, that.ts) &&
                Objects.equals(vc, that.vc);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, ts, vc);
    }
}
```
这里需要注意，我们定义的`WaterSensor`，有这样几个特点：

- 类是公有（public）的
- 有一个无参的构造方法
- 所有属性都是公有（public）的
- 所有属性的类型都是可以序列化的

Flink会把这样的类作为一种特殊的`POJO`（Plain Ordinary Java Object简单的Java对象，实际就是普通JavaBeans）数据类型来对待，方便数据的解析和序列化。另外我们在类中还重写了`toString`方法，主要是为了测试输出显示更清晰。
我们这里自定义的`POJO`类会在后面的代码中频繁使用，所以在后面的代码中碰到，把这里的`POJO`类导入就好了。
## 5.2.2 从集合中读取数据
最简单的读取数据的方式，就是在代码中直接创建一个Java集合，然后调用执行环境的`fromCollection`方法进行读取。这相当于将数据临时存储到内存中，形成特殊的数据结构后，作为数据源使用，一般用于测试。
```java
public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	List<Integer> data = Arrays.asList(1, 22, 3);
    DataStreamSource<Integer> ds = env.fromCollection(data);
	stream.print();
    env.execute();
}
```
## 5.2.3 从文件读取数据
真正的实际应用中，自然不会直接将数据写在代码中。通常情况下，我们会从存储介质中获取数据，一个比较常见的方式就是读取日志文件。这也是批处理中最常见的读取方式。
读取文件，需要添加文件连接器依赖:
```java
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-files</artifactId>
    <version>${flink.version}</version>
</dependency>
```
示例如下:
```java
public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("input/word.txt")).build();
    env
    	.fromSource(fileSource,WatermarkStrategy.noWatermarks(),"file")
    	.print(); 
    env.execute();
}
```
说明：

- 参数可以是目录，也可以是文件；还可以从HDFS目录下读取，使用路径hdfs://...；
- 路径可以是相对路径，也可以是绝对路径；
- 相对路径是从系统属性user.dir获取路径：idea下是project的根目录，standalone模式下是集群节点根目录；
## 5.2.4 从 Socket 读取数据
不论从集合还是文件，我们读取的其实都是有界数据。在流处理的场景中，数据往往是无界的。
我们之前用到的读取`socket`文本流，就是流处理场景。但是这种方式由于吞吐量小、稳定性较差，一般也是用于测试。
```java
DataStream<String> stream = env.socketTextStream("localhost", 7777);
```
## 5.2.5 从 Kafka 读取数据
Flink官方提供了连接工具flink-connector-kafka，直接帮我们实现了一个消费者`FlinkKafkaConsumer`，它就是用来读取Kafka数据的SourceFunction。
所以想要以Kafka作为数据源获取数据，我们只需要引入Kafka连接器的依赖。Flink官方提供的是一个通用的Kafka连接器，它会自动跟踪最新版本的Kafka客户端。目前最新版本只支持0.10.0版本以上的Kafka。这里我们需要导入的依赖如下。
```java
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka</artifactId>
    <version>${flink.version}</version>
</dependency>
```
代码如下：
```java
public class SourceKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers("hadoop102:9092")
            .setTopics("topic_1")
            .setGroupId("atguigu")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema()) 
            .build();

        DataStreamSource<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        stream.print("Kafka");

        env.execute();
    }
}
```
## 5.2.6 从数据生成器读取数据
Flink从1.11开始提供了一个内置的DataGen 连接器，主要是用于生成一些随机数，用于在没有数据源的时候，进行流任务的测试以及性能测试等。1.17提供了新的Source写法，需要导入依赖：
```java
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-datagen</artifactId>
    <version>${flink.version}</version>
</dependency>
```
代码如下：
```java
public class DataGeneratorDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataGeneratorSource<String> dataGeneratorSource =
                new DataGeneratorSource<>(
                        new GeneratorFunction<Long, String>() {
                            @Override
                            public String map(Long value) throws Exception {
                                return "Number:"+value;
                            }
                        },
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(10),
                        Types.STRING
                );

        env
                .fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "datagenerator")
                .print();

        env.execute();
    }
}
```
## 5.2.7 Flink 支持的数据类型 

1. **Flink的类型系统**

Flink使用“类型信息”（TypeInformation）来统一表示数据类型。TypeInformation类是Flink中所有类型描述符的基类。它涵盖了类型的一些基本属性，并为每个数据类型生成特定的序列化器、反序列化器和比较器。

2. **Flink支持的数据类型**

对于常见的Java和Scala数据类型，Flink都是支持的。Flink在内部，Flink对支持不同的类型进行了划分，这些类型可以在Types工具类中找到：
（1）基本类型
所有Java基本类型及其包装类，再加上`Void`、`String`、`Date`、`BigDecimal`和`BigInteger`。
（2）数组类型
包括基本类型数组（`PRIMITIVE_ARRAY`）和对象数组（`OBJECT_ARRAY`）。
（3）复合数据类型

- Java元组类型（TUPLE）：这是Flink内置的元组类型，是Java API的一部分。最多25个字段，也就是从Tuple0~Tuple25，不支持空字段。
- Scala 样例类及Scala元组：不支持空字段。
- 行类型（ROW）：可以认为是具有任意个字段的元组，并支持空字段。
- POJO：Flink自定义的类似于Java bean模式的类。

（4）辅助类型
Option、Either、List、Map等。
（5）泛型类型（GENERIC）
Flink支持所有的Java类和Scala类。不过如果没有按照上面POJO类型的要求来定义，就会被Flink当作泛型类来处理。Flink会把泛型类型当作黑盒，无法获取它们内部的属性；它们也不是由Flink本身序列化的，而是由Kryo序列化的。
在这些类型中，元组类型和POJO类型最为灵活，因为它们支持创建复杂类型。而相比之下，POJO还支持在键（key）的定义中直接使用字段名，这会让我们的代码可读性大大增加。所以，在项目实践中，往往会将流处理程序中的元素类型定为Flink的POJO类型。
Flink对POJO类型的要求如下：

- 类是公有（public）的
- 有一个无参的构造方法
- 所有属性都是公有（public）的
- 所有属性的类型都是可以序列化的
3. **类型提示（Type Hints）**

Flink还具有一个类型提取系统，可以分析函数的输入和返回类型，自动获取类型信息，从而获得对应的序列化器和反序列化器。但是，由于Java中泛型擦除的存在，在某些特殊情况下（比如Lambda表达式中），自动提取的信息是不够精细的——只告诉Flink当前的元素由“船头、船身、船尾”构成，根本无法重建出“大船”的模样；这时就需要显式地提供类型信息，才能使应用程序正常工作或提高其性能。
为了解决这类问题，Java API提供了专门的“类型提示”（type hints）。
回忆一下之前的word count流处理程序，我们在将String类型的每个词转换成（word， count）二元组后，就明确地用returns指定了返回的类型。因为对于map里传入的Lambda表达式，系统只能推断出返回的是Tuple2类型，而无法得到Tuple2<String, Long>。只有显式地告诉系统当前的返回类型，才能正确地解析出完整数据。
```java
.map(word -> Tuple2.of(word, 1L))
.returns(Types.TUPLE(Types.STRING, Types.LONG));
```
Flink还专门提供了`TypeHint`类，它可以捕获泛型的类型信息，并且一直记录下来，为运行时提供足够的信息。我们同样可以通过`.returns()`方法，明确地指定转换之后的DataStream里元素的类型。
```java
returns(new TypeHint<Tuple2<Integer, SomeType>>(){})
```
# 5.3 转换算子（Transformation）
数据源读入数据之后，我们就可以使用各种转换算子，将一个或多个`DataStream`转换为新的`DataStream`。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698417764998-27fdbfde-5f5d-4a8c-93f6-f7029a67c5df.png#averageHue=%23ebe0db&clientId=uceacbb65-3c84-4&from=paste&id=u94b1b8ba&originHeight=141&originWidth=1280&originalType=binary&ratio=2&rotation=0&showTitle=false&size=542537&status=done&style=none&taskId=u7883a35b-42f0-47de-943d-6a99168fca5&title=)
## 5.3.1 基本转换算子（map/filter/flatMap）
### 5.3.1.1 映射（map）
`map`是大家非常熟悉的大数据操作算子，主要用于将数据流中的数据进行转换，形成新的数据流。简单来说，就是一个“一一映射”，消费一个元素就产出一个元素。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698417807994-35be6b50-8251-4c7f-9c4f-0d02f27f8866.png#averageHue=%23fbf4ed&clientId=uceacbb65-3c84-4&from=paste&id=u2a11e37c&originHeight=286&originWidth=1280&originalType=binary&ratio=2&rotation=0&showTitle=false&size=1100383&status=done&style=none&taskId=u9a59f912-54c6-4a8f-bfce-7b55565593a&title=)
我们只需要基于`DataStream`调用`map()`方法就可以进行转换处理。方法需要传入的参数是接口`MapFunction`的实现；返回值类型还是`DataStream`，不过泛型（流中的元素类型）可能改变。
下面的代码用不同的方式，实现了提取`WaterSensor`中的`id`字段的功能。
```java
public class TransMap {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1, 1),
                new WaterSensor("sensor_2", 2, 2)
        );

        // 方式一：传入匿名类，实现MapFunction
        stream.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor e) throws Exception {
                return e.id;
            }
        }).print();

        // 方式二：传入MapFunction的实现类
        // stream.map(new UserMap()).print();

        env.execute();
    }

    public static class UserMap implements MapFunction<WaterSensor, String> {
        @Override
        public String map(WaterSensor e) throws Exception {
            return e.id;
        }
    }
}
```
上面代码中，`MapFunction`实现类的泛型类型，与输入数据类型和输出数据的类型有关。在实现`MapFunction`接口的时候，需要指定两个泛型，分别是输入事件和输出事件的类型，还需要重写一个`map()`方法，定义从一个输入事件转换为另一个输出事件的具体逻辑。
### 5.3.1.2 过滤（filter）
`filter`转换操作，顾名思义是对数据流执行一个过滤，通过一个布尔条件表达式设置过滤条件，对于每一个流内元素进行判断，若为true则元素正常输出，若为false则元素被过滤掉。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698419061345-086effd5-ccd6-48ff-84df-a0d57c2eca2d.png#averageHue=%23f8f2f0&clientId=uceacbb65-3c84-4&from=paste&id=ud55d05dc&originHeight=277&originWidth=1280&originalType=binary&ratio=2&rotation=0&showTitle=false&size=1065749&status=done&style=none&taskId=u45160907-4242-4599-ad79-c3ef2330f64&title=)
进行filter转换之后的新数据流的数据类型与原数据流是相同的。filter转换需要传入的参数需要实现`FilterFunction`接口，而`FilterFunction`内要实现`filter()`方法，就相当于一个返回布尔类型的条件表达式。
**案例需求：**下面的代码会将数据流中传感器id为sensor_1的数据过滤出来。
```java
public class TransFilter {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(
            new WaterSensor("sensor_1", 1, 1),
            new WaterSensor("sensor_1", 2, 2),
            new WaterSensor("sensor_2", 2, 2),
            new WaterSensor("sensor_3", 3, 3)
        );

        // 方式一：传入匿名类实现FilterFunction
        stream.filter(new FilterFunction<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor e) throws Exception {
                return e.id.equals("sensor_1");
            }
        }).print();

        // 方式二：传入FilterFunction实现类
        // stream.filter(new UserFilter()).print();
        
        env.execute();
    }
    public static class UserFilter implements FilterFunction<WaterSensor> {
        @Override
        public boolean filter(WaterSensor e) throws Exception {
            return e.id.equals("sensor_1");
        }
    }
}
```
### 5.3.1.3 扁平映射（flatMap）
`flatMap`操作又称为扁平映射，主要是将数据流中的整体（一般是集合类型）拆分成一个一个的个体使用。消费一个元素，可以产生0到多个元素。`flatMap`可以认为是“扁平化”（flatten）和“映射”（map）两步操作的结合，也就是先按照某种规则对数据进行打散拆分，再对拆分后的元素做转换处理。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698419110582-60cfef6b-81f4-44d9-af59-fe980315843b.png#averageHue=%23f8f0ed&clientId=uceacbb65-3c84-4&from=paste&height=179&id=u2bae90db&originHeight=357&originWidth=1280&originalType=binary&ratio=2&rotation=0&showTitle=false&size=1373530&status=done&style=none&taskId=u84e6a133-30e2-4d40-88d1-9e908f10151&title=&width=640)
同map一样，`flatMap`也可以使用Lambda表达式或者`FlatMapFunction`接口实现类的方式来进行传参，返回值类型取决于所传参数的具体逻辑，可以与原数据流相同，也可以不同。
**案例需求：**如果输入的数据是sensor_1，只打印vc；如果输入的数据是sensor_2，既打印ts又打印vc。
实现代码如下：
```java
public class TransFlatmap {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(      
            new WaterSensor("sensor_1", 1, 1),
            new WaterSensor("sensor_1", 2, 2),
            new WaterSensor("sensor_2", 2, 2),
            new WaterSensor("sensor_3", 3, 3)
        );

        stream.flatMap(new MyFlatMap()).print();

        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<WaterSensor, String> {

        @Override
        public void flatMap(WaterSensor value, Collector<String> out) throws Exception {

            if (value.id.equals("sensor_1")) {
                out.collect(String.valueOf(value.vc));
            } else if (value.id.equals("sensor_2")) {
                out.collect(String.valueOf(value.ts));
                out.collect(String.valueOf(value.vc));
            }
        }
    }
} 
```
## 5.3.2 聚合算子（Aggregation）
计算的结果不仅依赖当前数据，还跟之前的数据有关，相当于要把所有数据聚在一起进行汇总合并——这就是所谓的“聚合”（`Aggregation`），类似于`MapReduce`中的`reduce`操作。
### 5.3.2.1 按键分区（KeyBy）
对于Flink而言，DataStream是没有直接进行聚合的API的。因为我们对海量数据做聚合肯定要进行分区并行处理，这样才能提高效率。所以在Flink中，要做聚合，需要先进行分区；这个操作就是通过keyBy来完成的。
keyBy是聚合前必须要用到的一个算子。keyBy通过指定键（key），可以将一条流从逻辑上划分成不同的分区（partitions）。这里所说的分区，其实就是并行处理的子任务。
基于不同的key，流中的数据将被分配到不同的分区中去；这样一来，所有具有相同的key的数据，都将被发往同一个分区。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698419253078-70dee6f1-ade8-4986-bab5-e5488e8dad28.png#averageHue=%23faf3ee&clientId=uceacbb65-3c84-4&from=paste&height=140&id=ue6187f7a&originHeight=279&originWidth=1280&originalType=binary&ratio=2&rotation=0&showTitle=false&size=1073443&status=done&style=none&taskId=u1fb6ac1c-3856-4e67-b2a2-401e858e863&title=&width=640)
在内部，是通过计算key的哈希值（hash code），对分区数进行取模运算来实现的。所以这里key如果是POJO的话，必须要重写`hashCode()`方法。
`keyBy()`方法需要传入一个参数，这个参数指定了一个或一组key。有很多不同的方法来指定key：比如对于Tuple数据类型，可以指定字段的位置或者多个位置的组合；对于POJO类型，可以指定字段的名称（String）；另外，还可以传入Lambda表达式或者实现一个键选择器（KeySelector），用于说明从数据中提取key的逻辑。
我们可以以id作为key做一个分区操作，代码实现如下：
```java
public class TransKeyBy {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(
            new WaterSensor("sensor_1", 1, 1),
            new WaterSensor("sensor_1", 2, 2),
            new WaterSensor("sensor_2", 2, 2),
            new WaterSensor("sensor_3", 3, 3)
        );

        // 方式一：使用Lambda表达式
        KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(e -> e.id);

        // 方式二：使用匿名类实现KeySelector
        KeyedStream<WaterSensor, String> keyedStream1 = stream.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor e) throws Exception {
                return e.id;
            }
        });

        env.execute();
    }
}
```
需要注意的是，keyBy得到的结果将不再是DataStream，而是会将DataStream转换为KeyedStream。KeyedStream可以认为是“分区流”或者“键控流”，它是对DataStream按照key的一个逻辑分区，所以泛型有两个类型：除去当前流中的元素类型外，还需要指定key的类型。
KeyedStream也继承自DataStream，所以基于它的操作也都归属于DataStream API。但它跟之前的转换操作得到的`SingleOutputStreamOperator`不同，只是一个流的分区操作，并不是一个转换算子。KeyedStream是一个非常重要的数据结构，只有基于它才可以做后续的聚合操作（比如sum，reduce）。
### 5.3.2.2 简单聚合（sum/min/max/minBy/maxby）
有了按键分区的数据流`KeyedStream`，我们就可以基于它进行聚合操作了。Flink为我们内置实现了一些最基本、最简单的聚合API，主要有以下几种：

- sum()：在输入流上，对指定的字段做叠加求和的操作。
- min()：在输入流上，对指定的字段求最小值。
- max()：在输入流上，对指定的字段求最大值。
- minBy()：与min()类似，在输入流上针对指定字段求最小值。不同的是，min()只计算指定字段的最小值，其他字段会保留最初第一个数据的值；而minBy()则会返回包含字段最小值的整条数据。
- maxBy()：与max()类似，在输入流上针对指定字段求最大值。两者区别与min()/minBy()完全一致。

简单聚合算子使用非常方便，语义也非常明确。这些聚合方法调用时，也需要传入参数；但并不像基本转换算子那样需要实现自定义函数，只要说明聚合指定的字段就可以了。指定字段的方式有两种：指定位置，和指定名称。
对于元组类型的数据，可以使用这两种方式来指定字段。需要注意的是，元组中字段的名称，是以f0、f1、f2、…来命名的。
如果数据流的类型是POJO类，那么就只能通过字段名称来指定，不能通过位置来指定了。
```java
public class TransAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> stream = env.fromElements(
            new WaterSensor("sensor_1", 1, 1),
            new WaterSensor("sensor_1", 2, 2),
            new WaterSensor("sensor_2", 2, 2),
            new WaterSensor("sensor_3", 3, 3)
        );
        stream.keyBy(e -> e.id).max("vc");    // 指定字段名称
        env.execute();
    }
}
```
简单聚合算子返回的，同样是一个`SingleOutputStreamOperator`，也就是从KeyedStream又转换成了常规的DataStream。所以可以这样理解：keyBy和聚合是成对出现的，先分区、后聚合，得到的依然是一个DataStream。而且经过简单聚合之后的数据流，元素的数据类型保持不变。
一个聚合算子，会为每一个key保存一个聚合的值，在Flink中我们把它叫作“状态”（state）。所以每当有一个新的数据输入，算子就会更新保存的聚合结果，并发送一个带有更新后聚合值的事件到下游算子。对于无界流来说，这些状态是永远不会被清除的，所以我们使用聚合算子，应该只用在含有有限个key的数据流上。
### 5.3.2.3 归约聚合（reduce）
reduce可以对已有的数据进行归约处理，把每一个新输入的数据和当前已经归约出来的值，再做一个聚合计算。
reduce操作也会将`KeyedStream`转换为`DataStream`。它不会改变流的元素数据类型，所以输出类型和输入类型是一样的。
调用KeyedStream的reduce方法时，需要传入一个参数，实现`ReduceFunction`接口。接口在源码中的定义如下：
```java
public interface ReduceFunction<T> extends Function, Serializable {
    T reduce(T value1, T value2) throws Exception;
}
```
ReduceFunction接口里需要实现reduce()方法，这个方法接收两个输入事件，经过转换处理之后输出一个相同类型的事件。在流处理的底层实现过程中，实际上是将中间“合并的结果”作为任务的一个状态保存起来的；之后每来一个新的数据，就和之前的聚合状态进一步做归约。
我们可以单独定义一个函数类实现ReduceFunction接口，也可以直接传入一个匿名类。当然，同样也可以通过传入Lambda表达式实现类似的功能。
为了方便后续使用，定义一个`WaterSensorMapFunction`：
```java
public class WaterSensorMapFunction implements MapFunction<String,WaterSensor> {
    @Override
    public WaterSensor map(String value) throws Exception {
        String[] datas = value.split(",");
        return new WaterSensor(datas[0],Long.valueOf(datas[1]) ,Integer.valueOf(datas[2]) );
    }
}
```
案例：使用reduce实现max和maxBy的功能。
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env
   .socketTextStream("hadoop102", 7777)
   .map(new WaterSensorMapFunction())
   .keyBy(WaterSensor::getId)
   .reduce(new ReduceFunction<WaterSensor>()
   {
       @Override
       public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
           System.out.println("Demo7_Reduce.reduce");

           int maxVc = Math.max(value1.getVc(), value2.getVc());
           //实现max(vc)的效果  取最大值，其他字段以当前组的第一个为主
           //value1.setVc(maxVc);
           //实现maxBy(vc)的效果  取当前最大值的所有字段
           if (value1.getVc() > value2.getVc()){
               value1.setVc(maxVc);
               return value1;
           }else {
               value2.setVc(maxVc);
               return value2;
           }
       }
   })
   .print();
env.execute();
```
reduce同简单聚合算子一样，也要针对每一个key保存状态。因为状态不会清空，所以我们需要将reduce算子作用在一个有限key的流上。
## 5.3.3 用户自定义函数（UDF）
用户自定义函数（user-defined function，UDF），即用户可以根据自身需求，重新实现算子的逻辑。
用户自定义函数分为：函数类、匿名函数、富函数类。
### 5.3.3.1 函数类（Function Classes）
Flink暴露了所有UDF函数的接口，具体实现方式为接口或者抽象类，例如MapFunction、FilterFunction、ReduceFunction等。所以用户可以自定义一个函数类，实现对应的接口。
**需求：**用来从用户的点击数据中筛选包含“sensor_1”的内容：
**方式一：**实现FilterFunction接口
```java
public class TransFunctionUDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> stream = env.fromElements(     
            new WaterSensor("sensor_1", 1, 1),
            new WaterSensor("sensor_1", 2, 2),
            new WaterSensor("sensor_2", 2, 2),
            new WaterSensor("sensor_3", 3, 3)
        );
        DataStream<String> filter = stream.filter(new UserFilter());
        filter.print();
        env.execute();
    }

    public static class UserFilter implements FilterFunction<WaterSensor> {
        @Override
        public boolean filter(WaterSensor e) throws Exception {
            return e.id.equals("sensor_1");
        }
    }
}
```
**方式二：**通过匿名类来实现FilterFunction接口：
```java
DataStream<String> stream = stream.filter(new FilterFunction< WaterSensor>() {
    @Override
    public boolean filter(WaterSensor e) throws Exception {
        return e.id.equals("sensor_1");
    }
});
```
**方式二的优化：**为了类可以更加通用，我们还可以将用于过滤的关键字"home"抽象出来作为类的属性，调用构造方法时传进去。
```java
DataStreamSource<WaterSensor> stream = env.fromElements(        
    new WaterSensor("sensor_1", 1, 1),
    new WaterSensor("sensor_1", 2, 2),
    new WaterSensor("sensor_2", 2, 2),
    new WaterSensor("sensor_3", 3, 3)
);

DataStream<String> stream = stream.filter(new FilterFunctionImpl("sensor_1"));

public static class FilterFunctionImpl implements FilterFunction<WaterSensor> {
    private String id;

    FilterFunctionImpl(String id) { this.id=id; }

    @Override
    public boolean filter(WaterSensor value) throws Exception {
        return thid.id.equals(value.id);
    }
}
```
**方式三：**采用匿名函数（Lambda）
```java
public class TransFunctionUDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> stream = env.fromElements(     
            new WaterSensor("sensor_1", 1, 1),
            new WaterSensor("sensor_1", 2, 2),
            new WaterSensor("sensor_2", 2, 2),
            new WaterSensor("sensor_3", 3, 3)
        );    

        //map函数使用Lambda表达式，不需要进行类型声明
        SingleOutputStreamOperator<String> filter = stream.filter(sensor -> "sensor_1".equals(sensor.id));
        filter.print();
        env.execute();
    }
}
```
### 5.3.3.2 富函数类（Rich Function Classes）
“富函数类”也是DataStream API提供的一个函数类的接口，所有的Flink函数类都有其Rich版本。富函数类一般是以抽象类的形式出现的。例如：RichMapFunction、RichFilterFunction、RichReduceFunction等。
与常规函数类的不同主要在于，富函数类可以获取运行环境的上下文，并拥有一些生命周期方法，所以可以实现更复杂的功能。
Rich Function有生命周期的概念。典型的生命周期方法有：

- open()方法，是Rich Function的初始化方法，也就是会开启一个算子的生命周期。当一个算子的实际工作方法例如map()或者filter()方法被调用之前，open()会首先被调用。
- close()方法，是生命周期中的最后一个调用的方法，类似于结束方法。一般用来做一些清理工作。

需要注意的是，这里的生命周期方法，对于一个并行子任务来说只会调用一次；而对应的，实际工作方法，例如RichMapFunction中的map()，在每条数据到来后都会触发一次调用。
来看一个例子说明：
```java
public class RichFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env
            .fromElements(1,2,3,4)
            .map(new RichMapFunction<Integer, Integer>() {
                @Override
                public void open(Configuration parameters) throws Exception {
                    super.open(parameters);
                    System.out.println("索引是：" + getRuntimeContext().getIndexOfThisSubtask() + " 的任务的生命周期开始");
                }
    
                @Override
                public Integer map(Integer integer) throws Exception {
                    return integer + 1;
                }
    
                @Override
                public void close() throws Exception {
                    super.close();
                    System.out.println("索引是：" + getRuntimeContext().getIndexOfThisSubtask() + " 的任务的生命周期结束");
                }
            })
            .print();
        env.execute();
    }
}
```
## 5.3.4 物理分区算子（Physical Partitioning）
常见的物理分区策略有：随机分配（Random）、轮询分配（Round-Robin）、重缩放（Rescale）和广播（Broadcast）。
### 5.3.4.1 随机分区（shuffle）
最简单的重分区方式就是直接“洗牌”。通过调用DataStream的.shuffle()方法，将数据随机地分配到下游算子的并行任务中去。
随机分区服从均匀分布（uniform distribution），所以可以把流中的数据随机打乱，均匀地传递到下游任务分区。因为是完全随机的，所以对于同样的输入数据, 每次执行得到的结果也不会相同。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1699892871105-db98a725-5ead-4723-af10-b4e6b001ab3d.png#averageHue=%23f5ece6&clientId=u02044fff-ead5-4&from=paste&height=172&id=u10245a06&originHeight=343&originWidth=1280&originalType=binary&ratio=2&rotation=0&showTitle=false&size=1319667&status=done&style=none&taskId=uc4d299cd-d257-4760-b29c-b3a995a0ca9&title=&width=640)
经过随机分区之后，得到的依然是一个DataStream。
我们可以做个简单测试：将数据读入之后直接打印到控制台，将输出的并行度设置为2，中间经历一次shuffle。执行多次，观察结果是否相同。
```java
public class ShuffleExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		 env.setParallelism(2);

        DataStreamSource<Integer> stream = env.socketTextStream("hadoop102", 7777);;

        stream.shuffle().print()

        env.execute();
    }
}
```
### 5.3.4.2 轮询分区（Round-Robin）
轮询，简单来说就是“发牌”，按照先后顺序将数据做依次分发。通过调用DataStream的.rebalance()方法，就可以实现轮询重分区。rebalance使用的是Round-Robin负载均衡算法，可以将输入流数据平均分配到下游的并行任务中去。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1699892924382-d602b403-85d6-46f4-a97a-5e0434ffee7e.png#averageHue=%23e8e5e3&clientId=u02044fff-ead5-4&from=paste&height=189&id=u188ece23&originHeight=377&originWidth=1280&originalType=binary&ratio=2&rotation=0&showTitle=false&size=1450473&status=done&style=none&taskId=u83ff7939-3dc7-47a0-b3eb-bda0f78a1da&title=&width=640)
```java
stream.rebalance()
```
### 5.3.4.3 重缩放分区（rescale）
重缩放分区和轮询分区非常相似。当调用rescale()方法时，其实底层也是使用Round-Robin算法进行轮询，但是只会将数据轮询发送到下游并行任务的一部分中。rescale的做法是分成小团体，发牌人只给自己团体内的所有人轮流发牌。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1699892965656-ac9f00b5-530b-4236-8816-9e01fee6a4ac.png#averageHue=%23ebe8e7&clientId=u02044fff-ead5-4&from=paste&height=253&id=u37048f02&originHeight=506&originWidth=1280&originalType=binary&ratio=2&rotation=0&showTitle=false&size=1946769&status=done&style=none&taskId=u1c204441-aafe-4428-8923-5a4c2ca37a7&title=&width=640)
```java
stream.rescale()
```
### 5.3.4.4 广播（broadcast）
这种方式其实不应该叫做“重分区”，因为经过广播之后，数据会在不同的分区都保留一份，可能进行重复处理。可以通过调用DataStream的broadcast()方法，将输入数据复制并发送到下游算子的所有并行任务中去。
```java
stream.broadcast()
```
### 5.3.4.5 全局分区（global）
全局分区也是一种特殊的分区方式。这种做法非常极端，通过调用.global()方法，会将所有的输入流数据都发送到下游算子的第一个并行子任务中去。这就相当于强行让下游任务并行度变成了1，所以使用这个操作需要非常谨慎，可能对程序造成很大的压力。
```java
stream.global()
```
### 5.3.4.6 自定义分区（Custom）
当Flink提供的所有分区策略都不能满足用户的需求时，我们可以通过使用partitionCustom()方法来自定义分区策略。
**1）自定义分区器**
```java
public class MyPartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {
        return Integer.parseInt(key) % numPartitions;
    }
}
```
**2）使用自定义分区**
```java
public class PartitionCustomDemo {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(2);

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);

        DataStream<String> myDS = socketDS
                .partitionCustom(
                        new MyPartitioner(),
                        value -> value);  
        myDS.print();
        env.execute();
    }
}
```
## 5.3.5 分流
所谓“分流”，就是将一条数据流拆分成完全独立的两条、甚至多条流。也就是基于一个DataStream，定义一些筛选条件，将符合条件的数据拣选出来放到对应的流里。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1699893172826-d01e325e-fc75-4ea7-8ea5-08536b1f9642.png#averageHue=%23f1ebe9&clientId=u02044fff-ead5-4&from=paste&height=360&id=u1e3684d5&originHeight=720&originWidth=1280&originalType=binary&ratio=2&rotation=0&showTitle=false&size=2770068&status=done&style=none&taskId=uc2d9750d-be2f-4ef6-a9b0-d79b5171c37&title=&width=640)
### 5.3.5.1 简单实现
其实根据条件筛选数据的需求，本身非常容易实现：只要针对同一条流多次独立调用.filter()方法进行筛选，就可以得到拆分之后的流了。
**案例需求：**读取一个整数数字流，将数据流划分为奇数流和偶数流。
**代码实现：**
```java
public class SplitStreamByFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Integer> ds = env.socketTextStream("hadoop102", 7777)
                                                           .map(Integer::valueOf);
        //将ds 分为两个流 ，一个是奇数流，一个是偶数流
        //使用filter 过滤两次
        SingleOutputStreamOperator<Integer> ds1 = ds.filter(x -> x % 2 == 0);
        SingleOutputStreamOperator<Integer> ds2 = ds.filter(x -> x % 2 == 1);

        ds1.print("偶数");
        ds2.print("奇数");
        
        env.execute();
    }
}
```
这种实现非常简单，但代码显得有些冗余——我们的处理逻辑对拆分出的三条流其实是一样的，却重复写了三次。而且这段代码背后的含义，是将原始数据流stream复制三份，然后对每一份分别做筛选；这明显是不够高效的。我们自然想到，能不能不用复制流，直接用一个算子就把它们都拆分开呢？
### 5.3.5.2 使用侧输出流
关于处理函数中侧输出流的用法，我们已经在7.5节做了详细介绍。简单来说，只需要调用上下文ctx的.output()方法，就可以输出任意类型的数据了。而侧输出流的标记和提取，都离不开一个“输出标签”（OutputTag），指定了侧输出流的id和类型。
**代码实现：**将WaterSensor按照Id类型进行分流。
```java
public class SplitStreamByOutputTag {    
	public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 7777)
              .map(new WaterSensorMapFunction());

        OutputTag<WaterSensor> s1 = new OutputTag<>("s1", Types.POJO(WaterSensor.class)){};
        OutputTag<WaterSensor> s2 = new OutputTag<>("s2", Types.POJO(WaterSensor.class)){};
       //返回的都是主流
        SingleOutputStreamOperator<WaterSensor> ds1 = ds.process(new ProcessFunction<WaterSensor, WaterSensor>()
        {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

                if ("s1".equals(value.getId())) {
                    ctx.output(s1, value);
                } else if ("s2".equals(value.getId())) {
                    ctx.output(s2, value);
                } else {
                    //主流
                    out.collect(value);
                }

            }
        });

        ds1.print("主流，非s1,s2的传感器");
        SideOutputDataStream<WaterSensor> s1DS = ds1.getSideOutput(s1);
        SideOutputDataStream<WaterSensor> s2DS = ds1.getSideOutput(s2);

        s1DS.printToErr("s1");
        s2DS.printToErr("s2");
        
        env.execute();
 
	}
}
```
## 5.3.6 基本合流操作
在实际应用中，我们经常会遇到来源不同的多条流，需要将它们的数据进行联合处理。所以Flink中合流的操作会更加普遍，对应的API也更加丰富。
### 5.3.6.1 联合（Union）
最简单的合流操作，就是直接将多条流合在一起，叫作流的“联合”（union）。联合操作要求必须流中的数据类型必须相同，合并之后的新流会包括所有流中的元素，数据类型不变。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1699893328807-5754e706-2c6b-4246-8167-44416df9e532.png#averageHue=%23f1ece9&clientId=u02044fff-ead5-4&from=paste&height=337&id=ufb4838e7&originHeight=674&originWidth=1280&originalType=binary&ratio=2&rotation=0&showTitle=false&size=2593105&status=done&style=none&taskId=u96ea5400-7981-42e1-863d-fef802b7610&title=&width=640)
在代码中，我们只要基于DataStream直接调用.union()方法，传入其他DataStream作为参数，就可以实现流的联合了；得到的依然是一个DataStream：
```java
stream1.union(stream2, stream3, ...)
```
注意：union()的参数可以是多个DataStream，所以联合操作可以实现多条流的合并。
**代码实现：**我们可以用下面的代码做一个简单测试：
```java
public class UnionExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3);
        DataStreamSource<Integer> ds2 = env.fromElements(2, 2, 3);
        DataStreamSource<String> ds3 = env.fromElements("2", "2", "3");

        ds1.union(ds2,ds3.map(Integer::valueOf))
           .print();

        env.execute();
    }
}
```
### 5.3.6.2 连接（Connect）
流的联合虽然简单，不过受限于数据类型不能改变，灵活性大打折扣，所以实际应用较少出现。除了联合（union），Flink还提供了另外一种方便的合流操作——连接（connect）。
**1）连接流（ConnectedStreams）**
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1699893494958-7f53586f-46d1-4a2a-9685-e0db73875ac3.png#averageHue=%23100d0c&clientId=u02044fff-ead5-4&from=paste&height=362&id=u663c3f37&originHeight=724&originWidth=1422&originalType=binary&ratio=2&rotation=0&showTitle=false&size=204204&status=done&style=none&taskId=ucadb1c04-91cd-44a6-989f-dfc6d472e76&title=&width=711)
**代码实现：**需要分为两步：首先基于一条DataStream调用.connect()方法，传入另外一条DataStream作为参数，将两条流连接起来，得到一个ConnectedStreams；然后再调用同处理方法得到DataStream。这里可以的调用的同处理方法有.map()/.flatMap()，以及.process()方法。
```java
public class ConnectDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3);
//        DataStreamSource<String> source2 = env.fromElements("a", "b", "c");

        SingleOutputStreamOperator<Integer> source1 = env
                .socketTextStream("hadoop102", 7777)
                .map(i -> Integer.parseInt(i));

        DataStreamSource<String> source2 = env.socketTextStream("hadoop102", 8888);

        /**
         * TODO 使用 connect 合流
         * 1、一次只能连接 2条流
         * 2、流的数据类型可以不一样
         * 3、 连接后可以调用 map、flatmap、process来处理，但是各处理各的
         */
        ConnectedStreams<Integer, String> connect = source1.connect(source2);

        SingleOutputStreamOperator<String> result = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "来源于数字流:" + value.toString();
            }

            @Override
            public String map2(String value) throws Exception {
                return "来源于字母流:" + value;
            }
        });

        result.print();

        env.execute();    
    }
}
```
上面的代码中，ConnectedStreams有两个类型参数，分别表示内部包含的两条流各自的数据类型；由于需要“一国两制”，因此调用.map()方法时传入的不再是一个简单的MapFunction，而是一个CoMapFunction，表示分别对两条流中的数据执行map操作。这个接口有三个类型参数，依次表示第一条流、第二条流，以及合并后的流中的数据类型。需要实现的方法也非常直白：.map1()就是对第一条流中数据的map操作，.map2()则是针对第二条流。
**2）CoProcessFunction**
与CoMapFunction类似，如果是调用.map()就需要传入一个CoMapFunction，需要实现map1()、map2()两个方法；而调用.process()时，传入的则是一个CoProcessFunction。它也是“处理函数”家族中的一员，用法非常相似。它需要实现的就是processElement1()、processElement2()两个方法，在每个数据到来时，会根据来源的流调用其中的一个方法进行处理。
值得一提的是，ConnectedStreams也可以直接调用.keyBy()进行按键分区的操作，得到的还是一个ConnectedStreams：
```java
connectedStreams.keyBy(keySelector1, keySelector2);
```
这里传入两个参数keySelector1和keySelector2，是两条流中各自的键选择器；当然也可以直接传入键的位置值（keyPosition），或者键的字段名（field），这与普通的keyBy用法完全一致。ConnectedStreams进行keyBy操作，其实就是把两条流中key相同的数据放到了一起，然后针对来源的流再做各自处理，这在一些场景下非常有用。
**案例需求：**连接两条流，输出能根据id匹配上的数据（类似inner join效果）
```java
public class ConnectKeybyDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Tuple2<Integer, String>> source1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c")
        );
        DataStreamSource<Tuple3<Integer, String, Integer>> source2 = env.fromElements(
                Tuple3.of(1, "aa1", 1),
                Tuple3.of(1, "aa2", 2),
                Tuple3.of(2, "bb", 1),
                Tuple3.of(3, "cc", 1)
        );

        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connect = source1.connect(source2);

        // 多并行度下，需要根据 关联条件 进行keyby，才能保证key相同的数据到一起去，才能匹配上
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connectKey = connect.keyBy(s1 -> s1.f0, s2 -> s2.f0);

        SingleOutputStreamOperator<String> result = connectKey.process(
                new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {
                    // 定义 HashMap，缓存来过的数据，key=id，value=list<数据>
                    Map<Integer, List<Tuple2<Integer, String>>> s1Cache = new HashMap<>();
                    Map<Integer, List<Tuple3<Integer, String, Integer>>> s2Cache = new HashMap<>();

                    @Override
                    public void processElement1(Tuple2<Integer, String> value, Context ctx, Collector<String> out) throws Exception {
                        Integer id = value.f0;
                        // TODO 1.来过的s1数据，都存起来
                        if (!s1Cache.containsKey(id)) {
                            // 1.1 第一条数据，初始化 value的list，放入 hashmap
                            List<Tuple2<Integer, String>> s1Values = new ArrayList<>();
                            s1Values.add(value);
                            s1Cache.put(id, s1Values);
                        } else {
                            // 1.2 不是第一条，直接添加到 list中
                            s1Cache.get(id).add(value);
                        }

                        //TODO 2.根据id，查找s2的数据，只输出 匹配上 的数据
                        if (s2Cache.containsKey(id)) {
                            for (Tuple3<Integer, String, Integer> s2Element : s2Cache.get(id)) {
                                out.collect("s1:" + value + "<--------->s2:" + s2Element);
                            }
                        }
                    }

                    @Override
                    public void processElement2(Tuple3<Integer, String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                        Integer id = value.f0;
                        // TODO 1.来过的s2数据，都存起来
                        if (!s2Cache.containsKey(id)) {
                            // 1.1 第一条数据，初始化 value的list，放入 hashmap
                            List<Tuple3<Integer, String, Integer>> s2Values = new ArrayList<>();
                            s2Values.add(value);
                            s2Cache.put(id, s2Values);
                        } else {
                            // 1.2 不是第一条，直接添加到 list中
                            s2Cache.get(id).add(value);
                        }

                        //TODO 2.根据id，查找s1的数据，只输出 匹配上 的数据
                        if (s1Cache.containsKey(id)) {
                            for (Tuple2<Integer, String> s1Element : s1Cache.get(id)) {
                                out.collect("s1:" + s1Element + "<--------->s2:" + value);
                            }
                        }
                    }
                });

        result.print();

        env.execute();
    }
}
```
# 5.4 输出算子（Sink）
Flink作为数据处理框架，最终还是要把计算处理的结果写入外部存储，为外部应用提供支持。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1699893689726-956806b0-fffa-441a-8e66-0a68ed2adc00.png#averageHue=%23ece4e1&clientId=u528bbc21-2615-4&from=paste&height=76&id=uad8da31f&originHeight=151&originWidth=1280&originalType=binary&ratio=2&rotation=0&showTitle=false&size=581000&status=done&style=none&taskId=u7bba38ec-018f-40f6-a017-5be67e250f8&title=&width=640)
## 5.4.1 连接到外部系统
Flink的DataStream API专门提供了向外部写入数据的方法：addSink。与addSource类似，addSink方法对应着一个“Sink”算子，主要就是用来实现与外部系统连接、并将数据提交写入的；Flink程序中所有对外的输出操作，一般都是利用Sink算子完成的。
Flink1.12以前，Sink算子的创建是通过调用DataStream的.addSink()方法实现的。
```java
stream.addSink(new SinkFunction(…));
```
addSink方法同样需要传入一个参数，实现的是SinkFunction接口。在这个接口中只需要重写一个方法invoke()，用来将指定的值写入到外部系统中。这个方法在每条数据记录到来时都会调用。
Flink1.12开始，同样重构了Sink架构，
```java
stream.sinkTo(…)
```
当然，Sink多数情况下同样并不需要我们自己实现。之前我们一直在使用的print方法其实就是一种Sink，它表示将数据流写入标准控制台打印输出。Flink官方为我们提供了一部分的框架的Sink连接器。如下图所示，列出了Flink官方目前支持的第三方系统连接器：
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1699893748594-91677aa3-ef6b-491c-ae10-887049d4c4b6.png#averageHue=%23f9f9f9&clientId=u528bbc21-2615-4&from=paste&height=333&id=u5d9d1b61&originHeight=665&originWidth=1280&originalType=binary&ratio=2&rotation=0&showTitle=false&size=2558466&status=done&style=none&taskId=ue05bd752-b4ff-4f66-89e9-92cda32f6e1&title=&width=640)
我们可以看到，像Kafka之类流式系统，Flink提供了完美对接，source/sink两端都能连接，可读可写；而对于Elasticsearch、JDBC等数据存储系统，则只提供了输出写入的sink连接器。
除Flink官方之外，Apache Bahir框架，也实现了一些其他第三方系统与Flink的连接器。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1699893772507-e720e7df-587d-4251-8db2-f45aa44b55be.png#averageHue=%23fdfdfd&clientId=u528bbc21-2615-4&from=paste&height=341&id=u7c11d214&originHeight=682&originWidth=1280&originalType=binary&ratio=2&rotation=0&showTitle=false&size=2623869&status=done&style=none&taskId=ub89948ed-4ff1-4aea-add1-c68007801be&title=&width=640)
除此以外，就需要用户自定义实现sink连接器了。
## 5.4.2 输出到文件
Flink专门提供了一个流式文件系统的连接器：FileSink，为批处理和流处理提供了一个统一的Sink，它可以将分区文件写入Flink支持的文件系统。
FileSink支持行编码（Row-encoded）和批量编码（Bulk-encoded）格式。这两种不同的方式都有各自的构建器（builder），可以直接调用FileSink的静态方法：

- 行编码： FileSink.forRowFormat（basePath，rowEncoder）。
- 批量编码： FileSink.forBulkFormat（basePath，bulkWriterFactory）。

示例:
```java
public class SinkFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 每个目录中，都有 并行度个数的 文件在写入
        env.setParallelism(2);

        // 必须开启checkpoint，否则一直都是 .inprogress
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);


        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "Number:" + value;
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1000),
                Types.STRING
        );

        DataStreamSource<String> dataGen = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");

        // 输出到文件系统
        FileSink<String> fieSink = FileSink
                // 输出行式存储的文件，指定路径、指定编码
                .<String>forRowFormat(new Path("f:/tmp"), new SimpleStringEncoder<>("UTF-8"))
                // 输出文件的一些配置： 文件名的前缀、后缀
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("atguigu-")
                                .withPartSuffix(".log")
                                .build()
                )
                // 按照目录分桶：如下，就是每个小时一个目录
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                // 文件滚动策略:  1分钟 或 1m
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(1))
                                .withMaxPartSize(new MemorySize(1024*1024))
                                .build()
                )
                .build();


        dataGen.sinkTo(fieSink);

        env.execute();
    }
}
```
## 5.4.3 输出到Kafka
（1）添加Kafka 连接器依赖
由于我们已经测试过从Kafka数据源读取数据，连接器相关依赖已经引入，这里就不重复介绍了。
（2）启动Kafka集群
（3）编写输出到Kafka的示例代码
输出无key的record:
```java
public class SinkKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 如果是精准一次，必须开启checkpoint（后续章节介绍）
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);


        SingleOutputStreamOperator<String> sensorDS = env
                .socketTextStream("hadoop102", 7777);

        /**
         * Kafka Sink:
         * TODO 注意：如果要使用 精准一次 写入Kafka，需要满足以下条件，缺一不可
         * 1、开启checkpoint（后续介绍）
         * 2、设置事务前缀
         * 3、设置事务超时时间：   checkpoint间隔 <  事务超时时间  < max的15分钟
         */
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                // 指定 kafka 的地址和端口
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                // 指定序列化器：指定Topic名称、具体的序列化
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("ws")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                // 写到kafka的一致性级别： 精准一次、至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 如果是精准一次，必须设置 事务的前缀
                .setTransactionalIdPrefix("atguigu-")
                // 如果是精准一次，必须设置 事务超时时间: 大于checkpoint间隔，小于 max 15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10*60*1000+"")
                .build();


        sensorDS.sinkTo(kafkaSink);


        env.execute();
    }
}
```
自定义序列化器，实现带key的record:
```java
public class SinkKafkaWithKey {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.noRestart());


        SingleOutputStreamOperator<String> sensorDS = env
                .socketTextStream("hadoop102", 7777);


        /**
         * 如果要指定写入kafka的key，可以自定义序列化器：
         * 1、实现 一个接口，重写 序列化 方法
         * 2、指定key，转成 字节数组
         * 3、指定value，转成 字节数组
         * 4、返回一个 ProducerRecord对象，把key、value放进去
         */
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<String>() {

                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext context, Long timestamp) {
                                String[] datas = element.split(",");
                                byte[] key = datas[0].getBytes(StandardCharsets.UTF_8);
                                byte[] value = element.getBytes(StandardCharsets.UTF_8);
                                return new ProducerRecord<>("ws", key, value);
                            }
                        }
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("atguigu-")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "")
                .build();


        sensorDS.sinkTo(kafkaSink);


        env.execute();
    }
}
```
（4）运行代码，在Linux主机启动一个消费者，查看是否收到数据
[atguigu@hadoop102 ~]$ 
bin/kafka-console-consumer.sh --bootstrap-server hadoop102:9092 --topic ws
## 5.4.4 输出到MySQL（JDBC）
写入数据的MySQL的测试步骤如下。
（1）添加依赖
添加MySQL驱动：
```java
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>8.0.27</version>
</dependency>
```
官方还未提供flink-connector-jdbc的1.17.0的正式依赖，暂时从apache snapshot仓库下载，pom文件中指定仓库路径：
```java
<repositories>
    <repository>
        <id>apache-snapshots</id>
        <name>apache snapshots</name>
		<url>https://repository.apache.org/content/repositories/snapshots/</url>
    </repository>
</repositories>
```
添加依赖：
```java
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-jdbc</artifactId>
    <version>1.17-SNAPSHOT</version>
</dependency>
```
如果不生效，还需要修改本地maven的配置文件，mirrorOf中添加如下标红内容：
```java
<mirror>
    <id>aliyunmaven</id>
    <mirrorOf>*,!apache-snapshots</mirrorOf>
    <name>阿里云公共仓库</name>
    <url>https://maven.aliyun.com/repository/public</url>
</mirror>
```
（2）启动MySQL，在test库下建表ws
```java
mysql>     
CREATE TABLE `ws` (
  `id` varchar(100) NOT NULL,
  `ts` bigint(20) DEFAULT NULL,
  `vc` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
```
（3）编写输出到MySQL的示例代码
```java
public class SinkMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction());


        /**
         * TODO 写入mysql
         * 1、只能用老的sink写法： addsink
         * 2、JDBCSink的4个参数:
         *    第一个参数： 执行的sql，一般就是 insert into
         *    第二个参数： 预编译sql， 对占位符填充值
         *    第三个参数： 执行选项 ---》 攒批、重试
         *    第四个参数： 连接选项 ---》 url、用户名、密码
         */
        SinkFunction<WaterSensor> jdbcSink = JdbcSink.sink(
                "insert into ws values(?,?,?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        //每收到一条WaterSensor，如何去填充占位符
                        preparedStatement.setString(1, waterSensor.getId());
                        preparedStatement.setLong(2, waterSensor.getTs());
                        preparedStatement.setInt(3, waterSensor.getVc());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3) // 重试次数
                        .withBatchSize(100) // 批次的大小：条数
                        .withBatchIntervalMs(3000) // 批次的时间
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("000000")
                        .withConnectionCheckTimeoutSeconds(60) // 重试的超时时间
                        .build()
        );


        sensorDS.addSink(jdbcSink);


        env.execute();
    }
}
```
（4）运行代码，用客户端连接MySQL，查看是否成功写入数据。
## 5.4.5 自定义Sink输出
如果我们想将数据存储到我们自己的存储设备中，而Flink并没有提供可以直接使用的连接器，就只能自定义Sink进行输出了。与Source类似，Flink为我们提供了通用的SinkFunction接口和对应的RichSinkDunction抽象类，只要实现它，通过简单地调用DataStream的.addSink()方法就可以自定义写入任何外部存储。
```java
stream.addSink(new MySinkFunction<String>());
```
在实现SinkFunction的时候，需要重写的一个关键方法invoke()，在这个方法中我们就可以实现将流里的数据发送出去的逻辑。
这种方式比较通用，对于任何外部存储系统都有效；不过自定义Sink想要实现状态一致性并不容易，所以一般只在没有其它选择时使用。实际项目中用到的外部连接器Flink官方基本都已实现，而且在不断地扩充，因此自定义的场景并不常见。
