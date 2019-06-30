[![License](https://img.shields.io/github/license/yonna-framework/database.svg)](https://packagist.org/packages/yonna/database)
[![Repo Size](https://img.shields.io/github/repo-size/yonna-framework/database.svg)](https://packagist.org/packages/yonna/database)
[![Downloads](https://img.shields.io/packagist/dm/yonna/database.svg)](https://packagist.org/packages/yonna/database)
[![Version](https://img.shields.io/github/release/yonna-framework/database.svg)](https://packagist.org/packages/yonna/database)
[![Php](https://img.shields.io/packagist/php-v/yonna/database.svg)](https://packagist.org/packages/yonna/database)

## Yonna DB - ORM 库

```
Yonna是一个极其纯净的纯API-PHP框架.
database库是yonna最强大的DB-ORM
```

## 

#### 如何安装

##### 可以通过composer安装：`composer require yonna/database`

##### 可以通过git下载：`git clone https://github.com/yonna-framework/database.git`

> Yonna demo：[GOTO yonna](https://github.com/yonna-framework/yonna)

### ORM

> 连贯写法，全解耦，autoload后随意使用
>> DB自带各种分析优化，所以不再需要 Model层进行优化，可专心编写你的service(scope)

```php
DB::connect() // 默认 'default'
    ->table('system_data')
    ->field('key,value')
    ->in('key', $key)
    ->one();
    
DB::connect('mysql')
    ->table('system_data')
    ->field('key,value')
    ->in('key', $key）
    ->one();
    
DB::connect('pgsql')
    ->schemas('default')
    ->table('system_data')
    ->field('key,value')
    ->in('key', $key)
    ->multi();
    
DB::connect('mssql')
    ->schemas('default')
    ->table('system_data')
    ->field('key,value')
    ->in('key', $key)
    ->page();
    
DB::connect('sqlite')
    ->table('system_data')
    ->field('key,value')
    ->in('key', $key)
    ->count();
    
DB::connect('redis')->set('key', 1);
DB::connect('redis')->get('key');
DB::connect('redis')->incr('key');
DB::connect('redis')->decr('key');

DB::connect('redisCo')->set('swoole', 1);
DB::connect('redisCo')->get('swoole');
DB::connect('redisCo')->incr('swoole', 1.5);
DB::connect('redisCo')->decr('swoole', 2);

```

> 终结方法
>> one() multi() page() insert() insertAll() delete() count() sum() avg() min() max()

> schemas() / table() 方法会进行一次clear，所以请放在前面

##### 局部闭包（默认）
##### 等于 ( "a" = 1 or "b" = 1 ) and( "c" = 1 or "d" = 1 or "e" = 1 )
```php
DB::connect('mysql')
    ->table('demo')
    ->equalTo('a',1)
    ->equalTo('b',1)
    ->closure('or')
    ->equalTo('c',1)
    ->equalTo('d',1)
    ->equalTo('e',1)
    ->closure('or')
    ->one();
```
##### 全局闭包
##### 等于 (( "a" = 1 or "b" = 1 ) or "c" = 1 or "d" = 1 or "e" = 1 ) 
```php
DB::connect('pgsql')
    ->schemas('default')
    ->table('demo')
    ->equalTo('a',1)
    ->equalTo('b',1)
    ->closure('or')
    ->equalTo('c',1)
    ->equalTo('d',1)
    ->equalTo('e',1)
    ->closure('or',true)
    ->one();
```


##### 插入后可以获取 lastInsertId（在无序列表中自动获取可能会产生严重的错误）
```php
try {
    DB::connect('pgsql')->schemas('default')->table('data')->insert(['data' => 1]);
    $lastId = DB::lastInsertId();
} catch (\Exception $e) {
    return $this->error($e->getMessage());
}
```

##### 如果你只是想获取sql而不请求数据库，请使用 fetchSql()
```php
DB::connect()->table('demo')->equalTo('status',1)->fetchSql()->multi();
```

### JSON（pgsql）
#### 搜索例子1
```sql
select * from "default".system_data where (data#>>'{project_name,name}')::text like '%系统%';
```
#### 搜索例子2
```sql
select * from "default".system_data where ("data"->'server_pre_alert_limit'->'value')::text::int > 5;
```

### 前端json搜索语法
`
{project_name,name} % #TT || (({project_name,name} % #系统 || {project_name,name} != #xxx) && {server_pre_alert_limit,value} > #1 && ({server_pre_alert_limit,value} > #0 || {server_pre_alert_limit,value} < #100000000))
`
```
[  n   ] isNull
         {?,?,?} n #
[  !n  ] isNotNull
         {?,?,?} !n #
[  %   ] like
         {?,?,?} % #???
[  !%  ] notLike
         {?,?,?} !% #???
[  =   ] equalTo
         {?,?,?} = #???
[  >   ] greaterThan
         {?,?,?} > #???
[  <   ] lessThan
         {?,?,?} < #???
[  >=  ] greaterThanOrEqualTo
         {?,?,?} >= #???
[  <=  ] lessThanOrEqualTo
         {?,?,?} <= #???
[  <> , != ] notEqualTo
         {?,?,?} <> #???
         {?,?,?} != #???
[  ><  ] between
         {?,?,?} >< #???,???
[  !>< ] notBetween
         {?,?,?} !>< #???,???
[  *   ] any
         {?,?,?} * #???,???,???...
[  ^   ] in
         {?,?,?} ^ #???,???,???...
[  !^  ] notIn
         {?,?,?} !^ #???,???,???...
```