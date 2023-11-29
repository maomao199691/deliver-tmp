import redis
from data_gjjc import MAP_GJJC
# from redis_data.data_gmjjdm import MAP_GMJJDM
# from redis_data.data_ipcdldm import MAP_IPCDLDM
#from redis_data.data_sblb import MAP_SBLB
#from data_xzqh import MAP_XZQH

r = redis.StrictRedis(host="172.16.8.42",port=30079,db=0, password="123456")

__map_data={
    'data_gjjc':MAP_GJJC,
    # 'data_gmjjdm':MAP_GMJJDM,
    # 'data_ipcdldm':MAP_IPCDLDM,
    # 'data_sblb':MAP_SBLB,
    # 'data_xzqh':MAP_XZQH
}

def __cache_map_data(key,map_data):
    # 将Map数据存储到Redis缓存中
    if r.hlen(key):
        print('已缓存:{}'.format(key))
    else:
        r.hmset(key, map_data)

def get_value_from_cache(key,map_data_key):
    #print("获取：{}中key={} 的值".format(key,map_data_key))
    # 从Redis缓存中获取值
    if r.hlen(key):
        value = r.hget(key, map_data_key)
        if value is not None:
            # 如果找到缓存中的值，则返回解码后的结果
            return value.decode()
    else:
        __cache_map_data(key,__map_data[key])
        value = r.hget(key, map_data_key)
        if value is not None:
            # 如果找到缓存中的值，则返回解码后的结果
            return value.decode()
    # 返回None的示例
    return None

def get_value_from_cache_list(key,map_data_keys):
    value_list =[]
    if map_data_keys:
        for key_item in map_data_keys:
            value_list.append(get_value_from_cache(key,key_item))
    return value_list


def __init_cache():
    print("初始化缓存")
    for key,val in __map_data.items():
        __cache_map_data(key,val)

#__init_cache()
result = get_value_from_cache('data_gjjc','cn')
print(result)


res = r.hlen(123)
print(res)
