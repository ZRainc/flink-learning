在批处理统计中，我们可以等待一批数据都到齐后，统一处理。但是在实时处理统计中，我们是来一条就得处理一条，那么我们怎么统计最近一段时间内的数据呢？引入“窗口”。
所谓的“窗口”，一般就是划定的一段时间范围，也就是“时间窗”；对在这范围内的数据进行处理，就是所谓的窗口计算。所以窗口和时间往往是分不开的。接下来我们就深入了解一下Flink中的时间语义和窗口的应用。
# 6.1 窗口（Windows）
## 6.1.1 窗口的概念
Flink是一种流式计算引擎，主要是来处理无界数据流的，数据源源不断、无穷无尽。想要更加方便高效地处理无界流，一种方式就是将无限数据切割成有限的“数据块”进行处理，这就是所谓的“窗口”（Window）
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698301044917-d2d13a48-6616-4bae-a4fe-ba259da13dd9.png#averageHue=%233a3635&clientId=u0c58fdb1-73c7-4&from=paste&id=u535306a2&originHeight=768&originWidth=1440&originalType=binary&ratio=2&rotation=0&showTitle=false&size=160778&status=done&style=none&taskId=u76159938-000e-4d50-83ff-c5a23fccc28&title=)
**注意：**Flink中窗口并不是静态准备好的，而是动态创建——当有落在这个窗口区间范围的数据达到时，才创建对应的窗口。另外，这里我们认为到达窗口结束时间时，窗口就触发计算并关闭，事实上“触发计算”和“窗口关闭”两个行为也可以分开。
## 6.1.2 窗口的分类
我们在上一节举的例子，其实是最为简单的一种时间窗口。在Flink中，窗口的应用非常灵活，我们可以使用各种不同类型的窗口来实现需求。接下来我们就从不同的角度，对Flink中内置的窗口做一个分类说明。
### 按驱动类型
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698301163107-09cac652-1b4b-4d70-8652-55ba6d8fbb6e.png#averageHue=%232b2727&clientId=u0c58fdb1-73c7-4&from=paste&id=u5f7124bb&originHeight=752&originWidth=1446&originalType=binary&ratio=2&rotation=0&showTitle=false&size=188065&status=done&style=none&taskId=u39c01fc5-fa5a-4ea0-a8e4-22cc1a94897&title=)
### 按照窗口分配数据的规则分类
根据分配数据的规则，窗口的具体实现可以分为4类：滚动窗口（Tumbling Window）、滑动窗口（Sliding Window）、会话窗口（Session Window），以及全局窗口（Global Window）。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698301217937-676025ce-2e15-46b0-aef6-77b0407425f6.png#averageHue=%23040000&clientId=u0c58fdb1-73c7-4&from=paste&id=ua1c42dd0&originHeight=792&originWidth=1442&originalType=binary&ratio=2&rotation=0&showTitle=false&size=186994&status=done&style=none&taskId=ua5479ca9-744a-4b76-9131-2854778acb0&title=)

![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698301241019-9658f7cd-89fe-4ee0-926f-891e4125059e.png#averageHue=%230b0100&clientId=u0c58fdb1-73c7-4&from=paste&id=u59e9833c&originHeight=806&originWidth=1440&originalType=binary&ratio=2&rotation=0&showTitle=false&size=214372&status=done&style=none&taskId=u05ad1454-c280-4eb1-a8a7-057281682c9&title=)

![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698301264432-3ea62989-6a49-40ec-95ed-09f668d8f20e.png#averageHue=%23060000&clientId=u0c58fdb1-73c7-4&from=paste&id=u78c0e631&originHeight=798&originWidth=1440&originalType=binary&ratio=2&rotation=0&showTitle=false&size=213798&status=done&style=none&taskId=u9fc0a087-7818-4ebe-a326-7d605346953&title=)

![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698301463578-9e9579e0-d6b5-477c-a6b6-87599b34a282.png#averageHue=%23020000&clientId=u0c58fdb1-73c7-4&from=paste&id=u22043459&originHeight=746&originWidth=1442&originalType=binary&ratio=2&rotation=0&showTitle=false&size=140399&status=done&style=none&taskId=u18586c52-8d30-4419-b599-f0f17461284&title=)
## 6.1.3 窗口API概览
### 6.1.3.1 按键分区（Keyed）和非按键分区（Non-Keyed）
在定义窗口操作之前，首先需要确定，到底是基于按键分区（`Keyed`）的数据流`KeyedStream`来开窗，还是直接在没有按键分区的`DataStream`上开窗。也就是说，在调用窗口算子之前，是否有`keyBy`操作。

1. **按键分区窗口（Keyed Windows）**

经过按键分区`keyBy`操作后，数据流会按照key被分为多条逻辑流（logical streams），这就是`KeyedStream`。基于`KeyedStream`进行窗口操作时，窗口计算会在多个并行子任务上同时执行。相同key的数据会被发送到同一个并行子任务，而窗口操作会基于每个key进行单独的处理。所以可以认为，每个key上都定义了一组窗口，各自独立地进行统计计算。
在代码实现上，我们需要先对`DataStream`调用`.keyBy()`进行按键分区，然后再调用`.window()`定义窗口。
```java
stream.keyBy(...)
	.window(...)
```

2. **非按键分区（Non-Keyed Windows）**

如果没有进行`keyBy`，那么原始的`DataStream`就不会分成多条逻辑流。这时窗口逻辑只能在一个任务（task）上执行，就相当于并行度变成了1。
在代码中，直接基于`DataStream`调用`.windowAll()`定义窗口。
```java
stream.windowAll(...)
```
注意：对于非按键分区的窗口操作，手动调大窗口算子的并行度也是无效的，`windowAll`本身就是一个非并行的操作。
### 6.1.3.2 代码中窗口API的调用
窗口操作主要有两个部分：窗口分配器（`Window Assigners`）和窗口函数（`Window Functions`）。
```java
stream.keyBy(<key selector>)
	.window(<window assigner>)
	.aggregate(<window function>)
```
其中`.window()`方法需要传入一个窗口分配器，它指明了窗口的类型；而后面的`.aggregate()`方法传入一个窗口函数作为参数，它用来定义窗口具体的处理逻辑。窗口分配器有各种形式，而窗口函数的调用方法也不只`.aggregate()`一种，我们接下来就详细展开讲解。
## 6.1.4 窗口分配器
定义窗口分配器（`Window Assigners`）是构建窗口算子的第一步，它的作用就是定义数据应该被“分配”到哪个窗口。所以可以说，窗口分配器其实就是在指定窗口的类型。
窗口分配器最通用的定义方式，就是调用`.window()`方法。这个方法需要传入一个`WindowAssigner`作为参数，返回`WindowedStream`。如果是非按键分区窗口，那么直接调用`.windowAll()`方法，同样传入一个`WindowAssigner`，返回的是`AllWindowedStream`。
窗口按照驱动类型可以分成时间窗口和计数窗口，而按照具体的分配规则，又有滚动窗口、滑动窗口、会话窗口、全局窗口四种。除去需要自定义的全局窗口外，其他常用的类型Flink中都给出了内置的分配器实现，我们可以方便地调用实现各种需求。
### 6.1.4.1 时间窗口
时间窗口是最常用的窗口类型，又可以细分为滚动、滑动和会话三种。

1. **滚动处理时间窗口**

窗口分配器由类`TumblingProcessingTimeWindows`提供，需要调用它的静态方法`.of()`。
```java
stream.keyBy(...)
	.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
	.aggregate(...)
```
这里`.of()`方法需要传入一个Time类型的参数size，表示滚动窗口的大小，我们这里创建了一个长度为5秒的滚动窗口。
另外，`.of()`还有一个重载方法，可以传入两个Time类型的参数：size和offset。第一个参数当然还是窗口大小，第二个参数则表示窗口起始点的偏移量。

2. **滑动处理时间窗口**

窗口分配器由类`SlidingProcessingTimeWindows`提供，同样需要调用它的静态方法`.of()`。
```java
stream.keyBy(...)
	.window(SlidingProcessingTimeWindows.of(Time.seconds(10)，Time.seconds(5)))
	.aggregate(...)
```
这里`.of()`方法需要传入两个Time类型的参数：size和slide，前者表示滑动窗口的大小，后者表示滑动窗口的滑动步长。我们这里创建了一个长度为10秒、滑动步长为5秒的滑动窗口。
滑动窗口同样可以追加第三个参数，用于指定窗口起始点的偏移量，用法与滚动窗口完全一致。

3. **处理时间会话窗口**

窗口分配器由类`ProcessingTimeSessionWindows`提供，需要调用它的静态方法`.withGap()`或者`.withDynamicGap()`。
```java
stream.keyBy(...)
	.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
	.aggregate(...)
```
这里`.withGap()`方法需要传入一个Time类型的参数size，表示会话的超时时间，也就是最小间隔session gap。我们这里创建了静态会话超时时间为10秒的会话窗口。
另外，还可以调用`withDynamicGap()`方法定义 session gap 的动态提取逻辑。

4. **滚动事件时间窗口**

窗口分配器由类`TumblingEventTimeWindows`提供，用法与滚动处理事件窗口完全一致。
```java
stream.keyBy(...)
	.window(TumblingEventTimeWindows.of(Time.seconds(5)))
	.aggregate(...)
```

5. **滑动事件时间窗口**

窗口分配器由类`SlidingEventTimeWindows`提供，用法与滑动处理事件窗口完全一致。
```java
stream.keyBy(...)
	.window(SlidingEventTimeWindows.of(Time.seconds(10)，Time.seconds(5)))
	.aggregate(...)
```

6. **事件时间会话窗口**

窗口分配器由类`EventTimeSessionWindows`提供，用法与处理事件会话窗口完全一致。
```java
stream.keyBy(...)
	.window(EventTimeSessionWindows.withGap(Time.seconds(10)))
	.aggregate(...)
```
### 6.1.4.2 计数窗口
计数窗口概念非常简单，本身底层是基于全局窗口（`Global Window`）实现的。Flink为我们提供了非常方便的接口：直接调用`.countWindow()`方法。根据分配规则的不同，又可以分为滚动计数窗口和滑动计数窗口两类，下面我们就来看它们的具体实现。

1. **滚动计数窗口**

滚动计数窗口只需要传入一个长整型的参数size，表示窗口的大小。
```java
stream.keyBy(...)
	.countWindow(10)
```
我们定义了一个长度为10的滚动计数窗口，当窗口中元素数量达到10的时候，就会触发计算执行并关闭窗口。

2. **滑动计数窗口**

与滚动计数窗口类似，不过需要在`.countWindow()`调用时传入两个参数：size和slide，前者表示窗口大小，后者表示滑动步长。
```java
stream.keyBy(...)
	.countWindow(10，3)
```
我们定义了一个长度为10、滑动步长为3的滑动计数窗口。每个窗口统计10个数据，每隔3个数据就统计输出一次结果。

3. **全局窗口**

全局窗口是计数窗口的底层实现，一般在需要自定义窗口时使用。它的定义同样是直接调用`.window()`，分配器由`GlobalWindows`类提供。
```java
stream.keyBy(...)
	.window(GlobalWindows.create());
```
需要注意使用全局窗口，必须自行定义触发器才能实现窗口计算，否则起不到任何作用。
## 6.1.5 窗口函数
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698301991647-597e1351-0eeb-40d8-ac18-c2f70395e3a8.png#averageHue=%23100d0d&clientId=u0c58fdb1-73c7-4&from=paste&id=u3b3d282d&originHeight=794&originWidth=1444&originalType=binary&ratio=2&rotation=0&showTitle=false&size=133621&status=done&style=none&taskId=uf1370b81-ab9f-45b3-ba25-c50e2fedbc5&title=)
窗口函数定义了要对窗口中收集的数据做的计算操作，根据处理的方式可以分为两类：增量聚合函数和全窗口函数。下面我们来进行分别讲解。
### 6.1.5.1 增量聚合函数(ReduceFunction / AggregateFunction)
窗口将数据收集起来，最基本的处理操作当然就是进行聚合。我们可以每来一个数据就在之前结果上聚合一次，这就是“增量聚合”。
典型的增量聚合函数有两个：`ReduceFunction`和`AggregateFunction`。

1. **规约函数（ReduceFunction）**
```java
public class WindowReduceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction())
                .keyBy(r -> r.getId())
                // 设置滚动事件时间窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<WaterSensor>() {

                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        System.out.println("调用reduce方法，之前的结果:"+value1 + ",现在来的数据:"+value2);
                        return new WaterSensor(value1.getId(), System.currentTimeMillis(),value1.getVc()+value2.getVc());
                    }
                })
                .print();

        env.execute();
    }
```

2. **聚合函数（AggregateFunction）**

`ReduceFunction`可以解决大多数归约聚合的问题，但是这个接口有一个限制，就是聚合状态的类型、输出结果的类型都必须和输入数据类型一样。
`Flink Window API`中的`aggregate`就突破了这个限制，可以定义更加灵活的窗口聚合操作。这个方法需要传入一个`AggregateFunction`的实现类作为参数。
`AggregateFunction`可以看作是`ReduceFunction`的通用版本，这里有三种类型：输入类型（IN）、累加器类型（ACC）和输出类型（OUT）。输入类型IN就是输入流中元素的数据类型；累加器类型ACC则是我们进行聚合的中间状态类型；而输出类型当然就是最终计算结果的类型了。
接口中有四个方法：

- `createAccumulator()`：创建一个累加器，这就是为聚合创建了一个初始状态，每个聚合任务只会调用一次。
- `add()`：将输入的元素添加到累加器中。
- `getResult()`：从累加器中提取聚合的输出结果。
- `merge()`：合并两个累加器，并将合并后的状态作为一个累加器返回。

所以可以看到，`AggregateFunction`的工作原理是：首先调用`createAccumulator()`为任务初始化一个状态（累加器）；而后每来一个数据就调用一次`add()`方法，对数据进行聚合，得到的结果保存在状态中；等到了窗口需要输出时，再调用`getResult()`方法得到计算结果。很明显，与`ReduceFunction`相同，`AggregateFunction`也是增量式的聚合；而由于输入、中间状态、输出的类型可以不同，使得应用更加灵活方便。
代码实现如下：
```java
public class WindowAggregateDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction());


        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        // 1. 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<String> aggregate = sensorWS
                .aggregate(
                        new AggregateFunction<WaterSensor, Integer, String>() {
                            @Override
                            public Integer createAccumulator() {
                                System.out.println("创建累加器");
                                return 0;
                            }

                            @Override
                            public Integer add(WaterSensor value, Integer accumulator) {
                                System.out.println("调用add方法,value="+value);
                                return accumulator + value.getVc();
                            }

                            @Override
                            public String getResult(Integer accumulator) {
                                System.out.println("调用getResult方法");
                                return accumulator.toString();
                            }

                            @Override
                            public Integer merge(Integer a, Integer b) {
                                System.out.println("调用merge方法");
                                return null;
                            }
                        }
                );
        
        aggregate.print();

        env.execute();
    }
}
```
另外，Flink也为窗口的聚合提供了一系列预定义的简单聚合方法，可以直接基于`WindowedStream`调用。主要包括`.sum()`/`max()`/`maxBy()`/`min()`/`minBy()`，与`KeyedStream`的简单聚合非常相似。它们的底层，其实都是通过`AggregateFunction`来实现的。
### 6.1.5.2 全窗口函数(full window functions)
有些场景下，我们要做的计算必须基于全部的数据才有效，这时做增量聚合就没什么意义了；另外，输出的结果有可能要包含上下文中的一些信息（比如窗口的起始时间），这是增量聚合函数做不到的。
所以，我们还需要有更丰富的窗口计算方式。窗口操作中的另一大类就是全窗口函数。与增量聚合函数不同，全窗口函数需要先收集窗口中的数据，并在内部缓存起来，等到窗口要输出结果的时候再取出数据进行计算。
在Flink中，全窗口函数也有两种：`WindowFunction`和`ProcessWindowFunction`。

1. **窗口函数（WindowFunction）**

`WindowFunction`字面上就是“窗口函数”，它其实是老版本的通用窗口函数接口。我们可以基于`WindowedStream`调用`.apply()`方法，传入一个`WindowFunction`的实现类。
```java
stream
	.keyBy(<key selector>)
	.window(<window assigner>)
	.apply(new MyWindowFunction());
```
这个类中可以获取到包含窗口所有数据的可迭代集合（`Iterable`），还可以拿到窗口（`Window`）本身的信息。
不过`WindowFunction`能提供的上下文信息较少，也没有更高级的功能。事实上，它的作用可以被`ProcessWindowFunction`全覆盖，所以之后可能会逐渐弃用。

2. **处理窗口函数（ProcessWindowFunction）**

`ProcessWindowFunction`是`Window API`中最底层的通用窗口函数接口。之所以说它“最底层”，是因为除了可以拿到窗口中的所有数据之外，`ProcessWindowFunction`还可以获取到一个“上下文对象”（`Context`）。这个上下文对象非常强大，不仅能够获取窗口信息，还可以访问当前的时间和状态信息。这里的时间就包括了处理时间（`processing time`）和事件时间水位线（`event time watermark`）。这就使得`ProcessWindowFunction`更加灵活、功能更加丰富，其实就是一个增强版的`WindowFunction`。
事实上，`ProcessWindowFunction`是Flink底层API——处理函数（`process function`）中的一员。
代码实现如下：
```java
public class WindowProcessDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        // 1. 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<String> process = sensorWS
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                long count = elements.spliterator().estimateSize();
                                long windowStartTs = context.window().getStart();
                                long windowEndTs = context.window().getEnd();
                                String windowStart = DateFormatUtils.format(windowStartTs, "yyyy-MM-dd HH:mm:ss.SSS");
                                String windowEnd = DateFormatUtils.format(windowEndTs, "yyyy-MM-dd HH:mm:ss.SSS");

                                out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());
                            }
                        }
                );

        process.print();

        env.execute();
    }
}
```
### 6.1.5.3 **增量聚合和全窗口函数的结合使用**
在实际应用中，我们往往希望兼具这两者的优点，把它们结合在一起使用。Flink的`Window API`就给我们实现了这样的用法。
我们之前在调用`WindowedStream`的`.reduce()`和`.aggregate()`方法时，只是简单地直接传入了一个`ReduceFunction`或`AggregateFunction`进行增量聚合。除此之外，其实还可以传入第二个参数：一个全窗口函数，可以是`WindowFunction`或者`ProcessWindowFunction`。
```java
// ReduceFunction与WindowFunction结合
public <R> SingleOutputStreamOperator<R> reduce(
ReduceFunction<T> reduceFunction，WindowFunction<T，R，K，W> function) 

// ReduceFunction与ProcessWindowFunction结合
public <R> SingleOutputStreamOperator<R> reduce(
ReduceFunction<T> reduceFunction，ProcessWindowFunction<T，R，K，W> function)

// AggregateFunction与WindowFunction结合
public <ACC，V，R> SingleOutputStreamOperator<R> aggregate(
AggregateFunction<T，ACC，V> aggFunction，WindowFunction<V，R，K，W> windowFunction)

// AggregateFunction与ProcessWindowFunction结合
public <ACC，V，R> SingleOutputStreamOperator<R> aggregate(
AggregateFunction<T，ACC，V> aggFunction,
ProcessWindowFunction<V，R，K，W> windowFunction)
```
这样调用的处理机制是：基于第一个参数（增量聚合函数）来处理窗口数据，每来一个数据就做一次聚合；等到窗口需要触发计算时，则调用第二个参数（全窗口函数）的处理逻辑输出结果。需要注意的是，这里的全窗口函数就不再缓存所有数据了，而是直接将增量聚合函数的结果拿来当作了`Iterable`类型的输入。
具体实现代码如下：
```java
public class WindowAggregateAndProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction());


        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        // 1. 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        // 2. 窗口函数：
        /**
         * 增量聚合 Aggregate + 全窗口 process
         * 1、增量聚合函数处理数据： 来一条计算一条
         * 2、窗口触发时， 增量聚合的结果（只有一条） 传递给 全窗口函数
         * 3、经过全窗口函数的处理包装后，输出
         *
         * 结合两者的优点：
         * 1、增量聚合： 来一条计算一条，存储中间的计算结果，占用的空间少
         * 2、全窗口函数： 可以通过 上下文 实现灵活的功能
         */

//        sensorWS.reduce()   //也可以传两个

        SingleOutputStreamOperator<String> result = sensorWS.aggregate(
                new MyAgg(),
                new MyProcess()
        );

        result.print();



        env.execute();
    }

    public static class MyAgg implements AggregateFunction<WaterSensor, Integer, String>{

        @Override
        public Integer createAccumulator() {
            System.out.println("创建累加器");
            return 0;
        }


        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            System.out.println("调用add方法,value="+value);
            return accumulator + value.getVc();
        }

        @Override
        public String getResult(Integer accumulator) {
            System.out.println("调用getResult方法");
            return accumulator.toString();
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            System.out.println("调用merge方法");
            return null;
        }
    }

	 // 全窗口函数的输入类型 = 增量聚合函数的输出类型
    public static class MyProcess extends ProcessWindowFunction<String,String,String,TimeWindow>{

        @Override
        public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
            long startTs = context.window().getStart();
            long endTs = context.window().getEnd();
            String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
            String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

            long count = elements.spliterator().estimateSize();

            out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());

        }
    }
}
```
这里我们为了方便处理，单独定义了一个`POJO`类`UrlViewCount`来表示聚合输出结果的数据类型，包含了url、浏览量以及窗口的起始结束时间。用一个`AggregateFunction`来实现增量聚合，每来一个数据就计数加一；得到的结果交给`ProcessWindowFunction`，结合窗口信息包装成我们想要的`UrlViewCount`，最终输出统计结果。
## 6.1.6 其他API
对于一个窗口算子而言，窗口分配器和窗口函数是必不可少的。除此之外，Flink还提供了其他一些可选的API，让我们可以更加灵活地控制窗口行为。
### 6.1.6.1 触发器（Trigger）
触发器主要是用来控制窗口什么时候触发计算。所谓的“触发计算”，本质上就是执行窗口函数，所以可以认为是计算得到结果并输出的过程。
基于`WindowedStream`调用`.trigger()`方法，就可以传入一个自定义的窗口触发器`Trigger`。
```java
stream.keyBy(...)
	.window(...)
	.trigger(new MyTrigger())
```
### 6.1.6.2 移除器（Evictor）
移除器主要用来定义移除某些数据的逻辑。基于`WindowedStream`调用`.evictor()`方法，就可以传入一个自定义的移除器`Evictor`。`Evictor`是一个接口，不同的窗口类型都有各自预实现的移除器。
```java
stream.keyBy(...)
	.window(...)
	.evictor(new MyEvictor())
```
# 6.2 时间语义
## 6.2.1 Flink中的时间语义
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698301768980-cc44dc78-7a8a-421d-8b14-5aedd766dc18.png#averageHue=%231e1913&clientId=u0c58fdb1-73c7-4&from=paste&id=u943ad17b&originHeight=776&originWidth=1398&originalType=binary&ratio=2&rotation=0&showTitle=false&size=152982&status=done&style=none&taskId=uee4c7ff4-b171-4834-9cd5-3be890b56a1&title=)
## 6.2.2 哪种时间语义更重要

- **从星球大战说起**

为了更加清晰地说明两种语义的区别，我们来举一个非常经典的例子：电影《星球大战》。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698301845266-0d5e5339-fb62-4b47-8bbc-23be11e4adef.png#averageHue=%23f6f6f5&clientId=u0c58fdb1-73c7-4&from=paste&id=ue40dbd6d&originHeight=568&originWidth=1280&originalType=binary&ratio=2&rotation=0&showTitle=false&size=2913512&status=done&style=none&taskId=u03aaee16-5881-4a74-8687-ae5d348abb2&title=)
如上图所示，我们会发现，看电影其实就是处理影片中数据的过程，所以影片的上映时间就相当于“处理时间”；而影片的数据就是所描述的故事，它所发生的背景时间就相当于“事件时间”。两种时间语义都有各自的用途，适用于不同的场景。

- **数据处理系统中的时间语义**

在实际应用中，事件时间语义会更为常见。一般情况下，业务日志数据中都会记录数据生成的时间戳（timestamp），它就可以作为事件时间的判断基础。
在Flink中，由于处理时间比较简单，早期版本默认的时间语义是处理时间；而考虑到事件时间在实际应用中更为广泛，从Flink1.12版本开始，Flink已经将事件时间作为默认的时间语义了。
# 6.3 水位线（Watermark）
## 6.3.1 事件时间和窗口
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698307525890-43581ec6-b2eb-4e52-a060-2e0e5d51c0f1.png#averageHue=%230a0606&clientId=u5ffbfbc4-fb30-4&from=paste&id=u53c29d9d&originHeight=780&originWidth=1444&originalType=binary&ratio=2&rotation=0&showTitle=false&size=166422&status=done&style=none&taskId=u94ec8180-69e2-4921-bb21-6c8714909ec&title=)
## 6.3.2 什么是水位线
在Flink中，用来衡量事件时间进展的标记，就被称作“水位线”（Watermark）。
具体实现上，水位线可以看作一条特殊的数据记录，它是插入到数据流中的一个标记点，主要内容就是一个时间戳，用来指示当前的事件时间。而它插入流中的位置，就应该是在某个数据到来之后；这样就可以从这个数据中提取时间戳，作为当前水位线的时间戳了。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698307655490-a54886a9-8d44-4e60-8bf3-e4c3d1f24779.png#averageHue=%23252222&clientId=u5ffbfbc4-fb30-4&from=paste&id=u90721147&originHeight=814&originWidth=1326&originalType=binary&ratio=2&rotation=0&showTitle=false&size=130912&status=done&style=none&taskId=u0ea21542-a228-4858-9f83-6a13b9ae6f3&title=)

![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698307674754-8bf9aff0-1f03-495f-af28-4f634cc6fdf9.png#averageHue=%232b2726&clientId=u5ffbfbc4-fb30-4&from=paste&id=u0cf093d4&originHeight=798&originWidth=1416&originalType=binary&ratio=2&rotation=0&showTitle=false&size=165727&status=done&style=none&taskId=uc306538f-01a0-4391-8024-6fc16ad9c70&title=)
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698307698714-25bf62cc-fd91-4b24-b6ff-9a2b322705f7.png#averageHue=%231f1c1b&clientId=u5ffbfbc4-fb30-4&from=paste&id=ue69af8e3&originHeight=552&originWidth=1418&originalType=binary&ratio=2&rotation=0&showTitle=false&size=94528&status=done&style=none&taskId=ufef9d91d-1817-4c95-98d9-a6ca6e923c6&title=)
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698307725836-11ca228c-b50e-4f09-80b7-e34f31a17190.png#averageHue=%232d2827&clientId=u5ffbfbc4-fb30-4&from=paste&id=u198dc32f&originHeight=810&originWidth=1388&originalType=binary&ratio=2&rotation=0&showTitle=false&size=190518&status=done&style=none&taskId=u2db625ed-f084-4b69-b9e9-2da1d234d7b&title=)
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698307746982-2c90304a-1043-4b88-877e-b2b597c670b4.png#averageHue=%23070000&clientId=u5ffbfbc4-fb30-4&from=paste&id=u0e561f92&originHeight=732&originWidth=1360&originalType=binary&ratio=2&rotation=0&showTitle=false&size=182383&status=done&style=none&taskId=uab4b314e-b171-4fbc-88cb-ef32e77d127&title=)
## 6.3.3 水位线和窗口的工作原理
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698307922423-a8cb3b40-1288-48b2-b730-7f116c27c6ac.png#averageHue=%23181313&clientId=u5ffbfbc4-fb30-4&from=paste&id=uc68dd43a&originHeight=796&originWidth=1442&originalType=binary&ratio=2&rotation=0&showTitle=false&size=189078&status=done&style=none&taskId=u7c0ed29b-556c-4c86-b012-8c6eada553e&title=)
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698307953898-bd83efd6-e149-44ea-b2be-08ca42509171.png#averageHue=%23191515&clientId=u5ffbfbc4-fb30-4&from=paste&id=ub1217bb8&originHeight=726&originWidth=1440&originalType=binary&ratio=2&rotation=0&showTitle=false&size=162460&status=done&style=none&taskId=u3f41e0a7-2a31-41b4-a7ca-be9f05cb695&title=)
![image.png](https://cdn.nlark.com/yuque/0/2023/png/1587901/1698307979021-d3c9516a-0fd9-4505-a16b-950e11db5cbf.png#averageHue=%233a3535&clientId=u5ffbfbc4-fb30-4&from=paste&id=uebf36eea&originHeight=768&originWidth=1444&originalType=binary&ratio=2&rotation=0&showTitle=false&size=161484&status=done&style=none&taskId=uf21eb0f1-4ca4-444f-844b-2b2671886ad&title=)
**注意：**Flink中窗口并不是静态准备好的，而是动态创建——当有落在这个窗口区间范围的数据达到时，才创建对应的窗口。另外，这里我们认为到达窗口结束时间时，窗口就触发计算并关闭，事实上“触发计算”和“窗口关闭”两个行为也可以分开，这部分内容我们会在后面详述。
## 6.3.4 生成水位线
## 6.3.5 水位线的传递
## 6.3.6 迟到的数据处理
# 6.4 基于时间的合流——双流联结（Join）
## 6.4.1 窗口联结（Window Join）
## 6.4.2 间隔联结（Interval Join）
