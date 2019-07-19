package com.walle.redis;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.walle.redis.util.RedisClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

public class TestJedis
{
    public static void main(String[] args)
    {
        System.out.println("********操作String类型************");
        //操作String类型
        operateString();
        System.out.println("********操作Hash类型************");
        //操作Hash类型
        operateHash();
        System.out.println("********操作List类型************");
        //操作List类型
        operateList();
        System.out.println("********操作Set类型数据************");
        //操作Set类型数据
        operateSet();
        System.out.println("********操作ZSet类型数据************");
        //操作有序集合类型
        operateSortedSet();
        System.out.println("********Jedis事务处理************");
        //Jedis事务处理
        jedisTransaction();
    }

    /**
     * String类型基本操作
     */
    public static void operateString(){
        Jedis jedis= RedisClient.getJedis();
        try
        {
            //清空数据库（谨慎操作），这里只是为了便于观察输出结果
            //jedis.flushDB();
            //设置键值
            jedis.set("test", "testString");
            System.out.println("Save test value="+jedis.get("test"));
            //在键值后追加内容
            jedis.append("test", " this is append string");
            System.out.println("Append string to test:"+jedis.get("test"));
            //根据键获取值
            String test=jedis.get("test");
            System.out.println("find string test:"+test);
            //删除指定键
            jedis.del("test");
            System.out.println("delete string test:"+jedis.get("test"));
            //判断键是否存在
            boolean isNotExists=jedis.exists("test");
            System.out.println("test is Exists?:"+(isNotExists?"是":"否"));
            //如果键值是整数可以进行加减操作，否则会报错
            jedis.set("testInt", "0");
            jedis.incr("testInt");
            System.out.println("new testInt:"+jedis.get("testInt"));
            jedis.del("testInt");
            //设置键的生存时间
            jedis.set("testtest", "testTTL");
            jedis.expire("testtest", 30);
            Thread.sleep(10000);
            //获取键的剩余生存时间
            System.out.println(jedis.ttl("testtest"));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally{
            RedisClient.releaseConn(jedis);
        }
    }

    public static void operateHash(){
        Jedis jedis=RedisClient.getJedis();
        try
        {
            //清空数据库（谨慎操作），这里只是为了便于观察输出结果
            //jedis.flushDB();
            Map<String, String> map=new HashMap<String, String>();
            map.put("name", "张三");
            map.put("sex", "男");
            map.put("age", "24");
            //添加hash类型数据
            jedis.hmset("person", map);
            //获取该键包含的所有键值对
            System.out.println("add hash map to jedis:"+jedis.hgetAll("person"));
            //获取该键包含的指定键值
            System.out.println("get key's value:"+jedis.hget("person", "name"));
            //判断键是否存在
            boolean isExists=jedis.hexists("person", "professional");
            System.out.println("Key professional is in name?"+(isExists?"是":"否"));
            //获取该散列包含的键的个数
            long hlen=jedis.hlen("person");
            System.out.println("key person's length is:"+hlen);
            //向散列中添加键值
            jedis.hset("person", "professional", "软件工程师");
            System.out.println("get updated persion:"+jedis.hgetAll("person"));
            //如果键值是整型，可以加减该键值
            jedis.hincrBy("person", "age",2);
            System.out.println("get updated age:"+jedis.hget("person","age"));
            //删除散列中的键
            jedis.hdel("person", "professional");
            isExists=jedis.hexists("person", "professional");
            System.out.println("person's professional is exists?"+(isExists?"是":"否"));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally{
            RedisClient.releaseConn(jedis);
        }
    }

    public static void operateList(){
        Jedis jedis=RedisClient.getJedis();
        try
        {
            //清空数据库（谨慎操作），这里只是为了便于观察输出结果
            //jedis.flushDB();
            //先删除之前创建的List避免之前存入的值影响输出结果
            jedis.del("redisList");
            //从列表左侧增加元素，lpush()方法参数列表是可变参数
            jedis.lpush("redisList","Redis","Mysql");
            jedis.lpushx("redisList", "Oracle");
            //lpushx()和rpushx()方法只能插入已存在的List中，如果键不存在就不进行任何操作
            jedis.lpushx("RedisList", "Oracle");
            System.out.println("------"+jedis.lrange("redisList", 0, -1));
            //从列表右侧插入元素
            jedis.rpush("redisList", "Mongodb");
            jedis.rpushx("redisList", "DB2");
            //linsert()可以在指定值后插入元素，如果该元素有多个，只在第一个后面插入
            jedis.linsert("redisList", LIST_POSITION.AFTER, "Mysql", "Mysql");
            jedis.linsert("redisList", LIST_POSITION.AFTER, "Mysql", "DB2");
            System.out.println(jedis.lrange("redisList", 0, -1));
            //lrange()方法可以遍历List中的元素返回list，当开始坐标是0结束坐标是-1时表示遍历整个redisList
            List<String>redisList=jedis.lrange("redisList", 0, -1);
            System.out.print("Element in redisList:[");
            for (int i = 0; i < redisList.size(); i++ )
            {
                System.out.print(redisList.get(i)+" ");
            }
            System.out.println("]");
            //根据指定索引获取值，索引为正从左往右获取，索引为负从右向左获取
            String index2=jedis.lindex("redisList", 2);
            String index_2=jedis.lindex("redisList", -2);
            System.out.println("from left to right the index 2 is:"+index2);
            System.out.println("from right to left the index 2 is:"+index_2);
            //修改列表指定索引元素，若不存在则报错
            jedis.lset("redisList", 1, "updateValue");
            System.out.println("update index 1 value:"+jedis.lindex("redisList", 1));
            //删除列表左侧头部元素
            String lrem=jedis.lpop("redisList");
            System.out.println("Remove left top element:"+lrem);
            //删除列表右侧头部元素
            String rrem=jedis.rpop("redisList");
            System.out.println("Remove right top element:"+rrem);
            //去除索引范围外的元素
            String ltrim=jedis.ltrim("redisList", 1, 3);
            System.out.println("trim redisList 1-3 other element:"+ltrim);
            System.out.println("find redisList:"+jedis.lrange("redisList", 0, -1));
            //移出指定值的索引位置，如果count>0从左往右删除count个该元素，如果count=0删除列表中全部该元素，如果count<0从右往左删除count个该元素
            jedis.lrem("redisList", 1, "DB2");
            System.out.println("remove from left to right DB2 in redisList:"+jedis.lrange("redisList", 0, -1));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally{
            RedisClient.releaseConn(jedis);
        }
    }

    public static void operateSet(){
        Jedis jedis=RedisClient.getJedis();
        try
        {
            //清空数据库（谨慎操作）
            //jedis.flushDB();
            //添加元素，注意与List类型的区别，Set不会存储重复元素，比较适合做博客标签等应用场景
            jedis.sadd("redisSet", "Redis");
            jedis.sadd("redisSet", "Redis","Mysql");
            jedis.sadd("redisSet", "Redis","Mysql","Oracle","DB2");
            //查询
            Set<String> redisSet=jedis.smembers("redisSet");
            System.out.print("Element in set:[");
            Iterator<String> iterator=redisSet.iterator();
            while (iterator.hasNext())
            {
                System.out.print(iterator.next()+" ");
            }
            System.out.println("]");
            //Set集合元素个数
            long slen=jedis.scard("redisSet");
            System.out.println("redisSet's size is:"+slen);
            //判断元素是否存在于集合内
            boolean isExists=jedis.sismember("redisSet", "Mysql");
            System.out.println("Mysql is in redisSet?"+(isExists?"是":"否"));
            //集合运算
            //并集
            jedis.sadd("redisSet2", "Redis","Mysql","SqlServer");
            Set<String> unionSet=jedis.sunion("redisSet","redisSet2");
            System.out.println("union result:"+unionSet);
            //并集结果存入redisSet集合
            System.out.println("unionSet in Redis:"+jedis.sunionstore("unionSet", "redisSet","redisSet2"));
            //交集
            Set<String> interSet=jedis.sinter("redisSet","redisSet2");
            System.out.println("interSet result:"+interSet);
            //交集结果存入redisSet集合
            System.out.println("interSet in Redis:"+jedis.sinterstore("interSet", "redisSet","redisSet2"));
            //差集
            Set<String> diffSet=jedis.sdiff("redisSet","redisSet2");
            System.out.println("diffSet result:"+diffSet);
            //差集结果存入redisSet集合
            System.out.println("diffSet in Redis:"+jedis.sdiffstore("diffSet","redisSet","redisSet2"));
            //自交就相当于去除集合中所以元素
            interSet=jedis.sinter("interSet","interSet");
            //删除指定集合元素
            jedis.srem("redisSet", "Mysql");
            //将一个集合中的元素移入另一个集合中
            jedis.smove("redisSet", "redisSet2", "DB2");
            System.out.println("Element in redisSet is:"+jedis.smembers("redisSet"));
            System.out.println("Element in redisSet2 is:"+jedis.smembers("redisSet2"));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally{
            RedisClient.releaseConn(jedis);
        }
    }

    public static void operateSortedSet(){
        Jedis jedis=RedisClient.getJedis();
        try
        {
            //清空数据库（谨慎操作），这里只是为了便于观察输出结果
            //jedis.flushDB();
            //增加
            jedis.zadd("scores", 69,"zhangsan");
            jedis.zadd("scores", 83,"lisi");
            jedis.zadd("scores", 73,"wanger");
            //zadd()方法也有重载的传入map类型,分数是Double类型
            Map<String, Double> scoresMap=new HashMap<String, Double>();
            scoresMap.put("zhaosi", new Double(59));
            scoresMap.put("qianyi", new Double(99));
            jedis.zadd("scores",scoresMap);
            //查询
            System.out.println("按照分数从低到高查询zrange:"+jedis.zrange("scores", 0, -1));
            System.out.println("按照分数从高到低查询zrange:"+jedis.zrevrange("scores", 0, -1));
            //使用Set存储元组遍历元组内分数和元素
            Set<Tuple> sortSet=jedis.zrangeWithScores("scores", 0, -1);
            Iterator<Tuple>iterator=sortSet.iterator();
            while(iterator.hasNext()){
                Tuple tuple=iterator.next();
                System.out.println(tuple.getScore()+":"+tuple.getElement());
            }
            //根据分数范围查询元素(60<=score<=100)
            Set<String> zrangeByScore=jedis.zrangeByScore("scores", new Double(60), new Double(100));
            System.out.print("zrangeByScore(60-100):");
            for (Iterator<String> it=zrangeByScore.iterator();it.hasNext();)
            {
                System.out.print(it.next()+" ");
            }
            System.out.println();
            //查询指定zset键的元素个数
            long setcount=jedis.zcard("scores");
            //查询指定分数范围内(60<=score<=100)zset键的元素个数
            long rangeCount=jedis.zcount("scores", 60, 100);
            //查询指定元素的下标，不存在则返回null
            long zrank=jedis.zrank("scores", "zhangsan");
            //查询指定元素的分数，不存在则返回null
            Double zscore=jedis.zscore("scores", "zhangsan");
            System.out.println("scores's size:"+setcount+"\nrangeCount(60-100):"+rangeCount+"\nzrank(zhangsan):"+zrank+"\nzscore(zhangsan):"+zscore);
            //修改分数
            Double zincrby=jedis.zincrby("scores", 99, "zhangsan");
            System.out.println("zincrby:"+zincrby);
            //删除指定元素
            jedis.zrem("scores", "zhangsan");
            //根据分数删除
            jedis.zremrangeByScore("scores", 60, 80);
            System.out.println("scores's elements:"+jedis.zrange("scores", 0, -1));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally{
            RedisClient.releaseConn(jedis);
        }
    }

    /**
     * 执行逻辑：Jedis事务开始后，如果是Jedis内部方法错误，执行exec不影响其他正常语句的执行结果，执行成功的结果仍能提交到数据库。
     * 如果是Java语法错误比如被零除，就进入catch异常处理段，执行Jedis的discard()方法回滚所有事务
     * @see
     */
    public static void jedisTransaction(){
        Jedis jedis=RedisClient.getJedis();
        //开始事务，在执行exec之前都属于事务范围内
        Transaction tx=jedis.multi();
        boolean errFlag=false;
        try
        {
            tx.set("test1", "value1");
            tx.set("test2", "value2");
            //对字符串进行算术运算，Jedis内部方法异常
            tx.incrBy("test1",2);
            //下面的运行时异常会导致程序进入catch段，然后执行discard()回滚所有事务
            //int x=10/0; 可以解注这条语句查看执行结果，别忘了清空数据库测试
            System.out.println("提交事务");
            //Jedis内部方法异常，提交事务执行成功的结果会存入redis数据库，执行失败的不执行
            List<Object> list=tx.exec();
            //每条语句执行结果存入list中
            for (int i = 0; i < list.size(); i++ )
            {
                System.out.println("list:"+list.get(i));
            }
        }
        catch (Exception e)
        {
            errFlag=true;
            //discard()方法在发生异常时可以回滚事务
            tx.discard();
            e.printStackTrace();
        }
        finally{
            if (errFlag==true)
            {
                System.out.println("发生异常时提交事务");
                tx.exec();
            }
            RedisClient.releaseConn(jedis);
        }
    }
}
