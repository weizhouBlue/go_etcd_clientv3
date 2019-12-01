package go_etcd_clientv3
import (
    //"go.etcd.io/etcd/clientv3"
    "github.com/coreos/etcd/clientv3"
    //"go.etcd.io/etcd/mvcc/mvccpb"
    "github.com/coreos/etcd/mvcc/mvccpb"
    "github.com/coreos/etcd/clientv3/concurrency"
    //"go.etcd.io/etcd/pkg/transport"
    "github.com/coreos/etcd/pkg/transport"
    //"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
    "github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
    "time"
    "os"
    "context"
    "crypto/tls"
    "strings"
    "fmt"
)




var (
    dialTimeout    = 5 * time.Second
    requestTimeout = 10 * time.Second
)


type EventWatch = mvccpb.Event_EventType
const(
	EventPut EventWatch =0 
	EventDelete EventWatch =1
)

var (
    Global_Tls_cert string
    Global_Tls_ca string 
    Global_Tls_key string
    Global_endpoints []string
    EnableLog=false
)



type Client struct{
	Tls_cert string
	Tls_ca	string
	Tls_key string
	cli *clientv3.Client
}


func existFile( filePath string ) bool {
	   if info , err := os.Stat(filePath) ; err==nil {
		if ! info.IsDir() {
			return true
		}
    }
    return false
}

func log( format string, a ...interface{} ) (n int, err error) {
    if EnableLog {
        return fmt.Printf(format , a... )    
    }
    return  0,nil
}

//=====================================================


func (c *Client) Connect( endpoints []string  )  error {

	var tlsConfig *tls.Config
    var etcd_endpoints []string
    var etcd_ca string
    var etcd_key string
    var etcd_cert string

	if len(endpoints) ==0 && len(Global_endpoints)==0  {
		return   fmt.Errorf("inputted etcd server is empty")

	}else if len(endpoints) ==0 && len(Global_endpoints)!=0  {
        etcd_endpoints=Global_endpoints
        etcd_ca=Global_Tls_ca
        etcd_key=Global_Tls_key
        etcd_cert=Global_Tls_cert

    }else if len(endpoints) !=0 {
        etcd_endpoints=endpoints
        etcd_ca=c.Tls_ca
        etcd_key=c.Tls_key
        etcd_cert=c.Tls_cert
    } 


	if strings.Contains( etcd_endpoints[0] , "https" ) {
		if existFile( etcd_ca )==false   {
			return fmt.Errorf("error, no file cert-ca")
		}
		if existFile( etcd_key )==false   {
			return fmt.Errorf("error, no file cert-key")
		}
		if existFile( etcd_cert )==false   {
			return fmt.Errorf("error, no file cert-cert")
		}

	    tlsInfo := transport.TLSInfo{
	        CertFile:       etcd_cert ,
	        KeyFile:        etcd_key  ,
	        TrustedCAFile:  etcd_ca ,
	    }
	    info, err := tlsInfo.ClientConfig()
	    if err != nil {
	        return err
	    }
	    tlsConfig=info

	}

    cli, err := clientv3.New(clientv3.Config{
        Endpoints:   etcd_endpoints ,
        DialTimeout: dialTimeout ,
        TLS:         tlsConfig ,
    })
    if err != nil {
        return fmt.Errorf("error, failed to connect to the server")
    }
	c.cli=cli

    return nil
}



func (c *Client) Put( key string , value string  , lease_id ... clientv3.LeaseID ) error {

    if c.cli == nil {
    	return fmt.Errorf("CLient has not connect to the server")
    }

    if len(key)==0 {
        return fmt.Errorf("error, key is empty ")
    }

    var err error
    ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
    if len(lease_id) > 0 && int64(lease_id[0]) > 0 {
	    log( "put etcd  %s=%s with lease id=%v \n" , key ,value , lease_id[0] )
    	_, err = c.cli.Put(ctx , key , value, clientv3.WithLease( lease_id[0] ) )

    }else{
	    log( "put etcd  %s=%s \n" , key ,value  )
    	_, err = c.cli.Put( ctx , key , value )
    }
    cancel()
    if err != nil {
        switch err {
	        case context.Canceled:
    			log(  "ctx is canceled by another routine: %v\n", err)
	        case context.DeadlineExceeded:
	            log(  "ctx is attached with a deadline is exceeded: %v\n", err)
	        case rpctypes.ErrEmptyKey:
	            log(  "client-side error: %v\n", err)
	        default:
	            log(  "bad cluster endpoints, which are not etcd servers: %v\n", err)
        }
        return err 
    }
    
    return nil
}



// 如果key 不存在 ，则值为空串
func (c *Client) Get( key string) (string , error ) {
    log( "get etcd  key=%s \n" , key  )

    if c.cli == nil {
    	return "" , fmt.Errorf("CLient has not connect to the server")
    }

    if len(key)==0 {
        return "" , fmt.Errorf( "error, key is empty "  ) 
    }

    ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
    resp, err := c.cli.Get(ctx, key )
    cancel()
    log( "response=%v \n" , resp)

    if err != nil  {
        return "", fmt.Errorf( "failed to get etcd key=%s , info=%v " , key , err )
    }


    if len(resp.Kvs)==0 {
        return "" , nil
    }else{
        result := resp.Kvs[0].Value     
        return string(result) , nil
    }

}



// 如果key 不存在 ，则值为空串
func (c *Client) GetListKey( keyList []string) ( map[string] string , error ) {
    log( "get etcd  keyList=%+v \n" , keyList  )

    if c.cli == nil {
        return nil , fmt.Errorf("CLient has not connect to the server " ) 
    }

    if len(keyList)==0 {
        return nil , fmt.Errorf("error, keyList is empty " )
    }
    for _ , v := range keyList {
        if len(v)==0 {
            return nil , fmt.Errorf("error, there is an empty key inputted ")
        }
    }

    reList:= map[string]string {}

    for _ , v := range keyList {
        if result , err := c.Get(v) ; err!=nil  {
            return nil ,  fmt.Errorf(  "error, failed to get key=%s  , info=%v " , v , err )
        }else{
            reList[v]=result
        }
    }
    return reList , nil

}


//---------------------------------------

func (c *Client) GetPrefix( prefix string) ( map[string]string  , error  ) {
    log( "GetPrefix etcd  prefix=%s \n" , prefix  )

    if c.cli == nil {
        return nil , fmt.Errorf("CLient has not connect to the server " ) 
    }

    if len(prefix)==0 {
        return nil , fmt.Errorf("error, prefix is empty " ) 
    }


    ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
    resp, err := c.cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
    cancel()
    log( "response=%v \n" , resp)

    if err != nil {
        return nil , err
    }
    if len(resp.Kvs)==0 {
        return nil , nil
    }

    result := make( map[string] string ) 
    for _, ev := range resp.Kvs {
        log( "GetPrefix get %s : %s  reversion=%d \n  ", ev.Key, ev.Value , resp.Header.Revision )
        result[ string(ev.Key) ]= string(ev.Value)
    }
    log( "succeeded to GetPrefix %v \n"  , result )
    return result , nil
}



//---------------------------------------


// 根据 []string  作为各个层级的 目录名和文件名 ，记录到 record 的 对应层级中
func recordToResult( dirList []string ,  val string  , record  map[string] interface{} ) error {

    // 合并 两个  map[string] interface{}  的内容
    insertNewItem:=func (  old  map[string] interface{} , newItem map[string] interface{} ) map[string] interface{} {
        newone:=map[string] interface{} {}
        for k , v :=range old {
            newone[k]=v
        }
        for k , v :=range newItem {
            newone[k]=v
        }    
        return newone
    }

    if len(dirList)==1 {
        keyname:=dirList[0] 
        if v , ok := record[keyname] ; ok {
            // 发现 有 /a/b=100 , /a/b/c=20 的情况，即 第2个key把b当做目录，而第一个不是
            return fmt.Errorf("error, %s is already a directory name with content \"%v\" , ignore setting to \"%v\" " , keyname ,v ,val )
        }
        record[keyname]=val

    }else{
        dirName:=dirList[0]
        tmp:=map[string] interface{} {}
        if err:=recordToResult( dirList[1:] , val , tmp ) ; err!=nil {
            return err
        }
        if _ , ok := record[dirName] ; ok{
            if t , ok := record[dirName].( map[string] interface{}  ) ; ok {
                record[dirName]= insertNewItem(t , tmp)
            }
        }else{
            record[dirName]=tmp
        }
    }

    return nil
}


/*
针对 etcd 上的 各个key 是按照 层级 定义的 ， 进行 解析，返回带有层级的 object
for example: etcd上 多个 key 和 其值 为如下 
    /t/a/b3  500
    /t/a/b2  400
    /t/a/b   300
    /t/mm/b5 500
那么，本函数 获取prefix=/t , 按照key的目录级别(去掉前缀) , 返回  
    map[
        a:map[ b:300 b2:400 b3:500 ] 
        mm:map[b5:500] 
        ]

    注意，如果有两个key 中，对于某个层级是 目录还是最终的文件名  出现了分歧，会自动 忽略 其作为 文件的 case
        /t/mm/b5 500
        /t/mm    600      这种key会被忽略记录    
*/

func (c *Client) GetPrefixReturnObj( prefix string , ignoreErrKey bool ) ( map[string] interface{}  , error  ) {

    // 返回的结果中，保障 re 的结果是 有序排列
    re , err := c.GetPrefix(prefix )
    if err!=nil {
        return nil , err
    }
    log( "after getting from etcd=%v \n " , re )

    result:=map[string] interface{} {}
    log( "begin to parse object \n " )
    for key , val := range re {
        log("parse %v=%v \n"  , key , val)

        if strings.HasSuffix( key , "/" ) {
            //  key 不能是  /a/b/c/  这种格式，即最后没有文件名
            if ignoreErrKey==true {
                fmt.Printf("error, ignore key=%s , errinfo=illeage format \n" , key  )
            }else{
                return nil , fmt.Errorf("error, found an key with illeage format, %s " , key )                
            }
        } 
        if strings.Contains( key , "//" ) {
            //  key 不能是  /a//b/c/  这种格式，即层级路径 错误
            if ignoreErrKey==true {
                fmt.Printf("error, ignore key=%s , errinfo=illeage format // \n" , key  )
            }else{
                return nil , fmt.Errorf("error, found an key with illeage format, %s " , key )                
            }
        }

        newKey:=strings.TrimPrefix( key , prefix )
        newKey=strings.TrimPrefix( newKey , "/" )

        if err:=recordToResult( strings.Split(newKey , "/") , val , result ) ; err!=nil {
            // 多个key ， 把 某个层级 作为 目录还是文件名 ， 出现冲突
            if ignoreErrKey==true {
                fmt.Printf("error, ignore key=%s , errinfo=%v \n" , key , err )
            }else{
                return nil , fmt.Errorf("error, key=%s confict with other key " , key )                
            }

        }
    }
    
    return  result , nil

}


//---------------------------------------

/*
以每个 key 的最后一层名字 作为 key名

针对 etcd 上的 各个key 是按照 /  层级 定义的 ， 进行 解析，返回一个字典，每个key是 最后的名字
for example: etcd上 多个 key 和 其值 为如下 
    /t/a/b3  500
    /t/a/b2  400
    /t/a/b   300
    /t/a     350
    /t/mm/b5 500

那么，按照 /t 的前缀  返回
    map[ b3:500 b2:400 b:300 b5:500 a:350 ]

    注意，不能出现如下这种key , 即 最后的 / 的没有名字
        /t/mm/  500
*/
func (c *Client) GetPrefixReturnEndName( prefix string  , ignoreErrKey bool  ) ( map[string] string   , error  ) {

    // 返回的结果中，保障 re 的结果是 有序排列
    re , err := c.GetPrefix(prefix )
    if err!=nil {
        return nil , err
    }
    log( "after getting from etcd=%v \n " , re )

    result:=map[string] string {}
    log( "begin to parse object \n " )
    for k , v := range re {
    log( "%v=%v\n " , k , v  )

        tmp:=strings.Split(k,"/")
        if len(tmp)==0  {
            if ignoreErrKey{
                continue
            }else{
                return nil , fmt.Errorf("error, invalid key=%s  " , k ) 
            }
        }
        endName:=tmp[ len(tmp) -1 ]
        if len(endName) == 0 {
            if ignoreErrKey{
                continue
            }else{
                return nil , fmt.Errorf("error, invalid key=%s  " , k ) 
            }
        }
        result[endName]=v
    }
    return result , nil
}




//---------------------------------------


/*
返回前缀的  指定 层级下的 目录名 和 键
其中，level 从 1 开始

for example: etcd上 多个 key 和 其值 为如下 
    /t/b/b3/b4  500
    /t/b/b2  400
    /t/a     300
    /t/a/a1  200
    /t/mm    500

那么， 按照前缀 /t ， level=1 来调用函数，那么2个结构
    第一个是 目录列表：  list[ b , a ]
    第二个是 keys字典：  map[a:"300" , mm:"500"]


那么， 按照前缀 /t ， level=2 来调用函数，那么2个结构
    第一个是 目录列表：  list[ b3 ]
    第二个是 keys字典：  map[b2:"400" , a1:"200"]


    注意，不能出现如下这种key , 即 最后的 / 的没有名字
        /t/mm/  500
*/



func (c *Client) GetPrefixReturnLevelName( prefix string  , level int , ignoreErrKey bool  ) ( dirs []string , keys map[string] string   , er error  ) {

    if level==0 {
        return nil, nil , fmt.Errorf("error, level must be > 0 "  )
    }

    // 返回的结果中，保障 re 的结果是 有序排列
    re , err := c.GetPrefix(prefix )
    if err!=nil {
        return nil, nil , err
    }
    log( "after getting from etcd=%v \n " , re )

    tmp_dirs:=map[string]string {}
    dirs=[]string {}
    keys=map[string] string {}

    log( "begin to parse object \n " )
    for k , v := range re {

        k=strings.TrimPrefix( k , prefix )
        k=strings.TrimPrefix( k , "/" )
        log( "%v=%v\n " , k , v  )

        tmp:=strings.Split(k,"/")
        n:=len(tmp)
        if n==0 {
            if ignoreErrKey{
                continue
            }else{
                return nil, nil , fmt.Errorf("error, invalid key=%s  " , k ) 
            }

        }else{
            if n<level {
                log(" ignore key= %s \n" , k)
                continue
            }else{
                keyName:=tmp[level-1]
                if len(keyName)==0 {
                    if ignoreErrKey{
                        continue
                    }else{
                        return nil, nil , fmt.Errorf("error, invalid key=%s  " , k ) 
                    }
                }

                if n==level  {
                    keys[keyName]=v
                }else {
                    tmp_dirs[keyName]=""
                }
            }
        }
    }

    for k , _ := range tmp_dirs {
        dirs=append(dirs,k)
    }

    return
}



/*
返回前缀的第一层级下的 目录名 和 键

针对 etcd 上的 各个key 是按照 /  层级 定义的 ， 进行 解析，返回一个字典，每个key是 第一层的名字
for example: etcd上 多个 key 和 其值 为如下 
    /t/b/b3  500
    /t/b/b2  400
    /t/a     300
    /t/a/a1  200
    /t/mm    500

那么， 按照前缀 /t 来调用函数，那么2个结构
    第一个是 目录列表：  list[ b , a ]
    第二个是 keys字典：  map[a:"300" , mm:"500"]

    注意，不能出现如下这种key , 即 最后的 / 的没有名字
        /t/mm/  500
*/


func (c *Client) GetPrefixReturnTopName( prefix string  , ignoreErrKey bool  ) ( dirs []string , keys map[string] string   , er error  ) {

    return c.GetPrefixReturnLevelName( prefix  , 1 , ignoreErrKey   )

}



//---------------------------------------


func (c *Client) Delete( key string , prefixFlag bool  ) error {
    log( "delete etcd  key=%s \n" , key  )

    if c.cli == nil {
        return   fmt.Errorf("CLient has not connect to the server " ) 
    }


    if len(key)==0 {
        return fmt.Errorf("error, key is empty " )  
    }


    var err error
    ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
    if prefixFlag {
    	_, err = c.cli.Delete(ctx, key , clientv3.WithPrefix())

    }else{
    	_, err = c.cli.Delete(ctx, key )
    }
    cancel()
    if err != nil {
        return err
    }
    return nil
}




// type ClientWatchChan = clientv3.WatchChan

// func (c *Client) Watch( key string , prefixFlag bool )  ClientWatchChan {
//     log( "Watch etcd  key=%s \n" , key  )

//     if c.cli == nil {
//     	log( "CLient has not connect to the server \n" )    
//     	return nil
//     }

//     if prefixFlag {
//     	return c.cli.Watch(context.Background(), key , clientv3.WithPrefix())
//     }else{
//     	return c.cli.Watch( context.Background() , key )
//     }
// }


//=====================================================



func (c *Client) WatchByHandler( key string , prefixFlag , ignoreDeleteEvent , ignorePutEvent bool , caller func(evnet EventWatch , key , newVal , oldVal string ) )  (ch_stop chan bool , err error ){
    log( "Watch etcd  key=%s \n" , key  )

    if c.cli == nil {
    	return nil ,  fmt.Errorf("CLient has not connect to the server " )  
    }

    if len(key)==0 {
        return nil , fmt.Errorf( "error, key is empty " ) 
    }

    opList:=[]clientv3.OpOption{}
    if prefixFlag {
        opList=append(opList , clientv3.WithPrefix() )
    }
    if ignoreDeleteEvent {
        opList=append(opList , clientv3.WithFilterDelete() )
    }
    if ignorePutEvent {
        opList=append(opList , clientv3.WithFilterPut() )
    }

    var eventChan clientv3.WatchChan
    eventChan = c.cli.Watch(context.Background(), key , opList... )

    ch_stop = make(chan bool)
    go func(c *Client){
    	for{
    		select {
    			case <-ch_stop :
    				log( "end watching , receive signal to stop watch, for %+v[prefix=%+v] \n" , key , prefixFlag)    
    				c.cli.Watcher.Close()
    				return 

    			case data , ok := <- eventChan :
    				if ok {
				        if data.Canceled {
    						log( "end watching , Watch was interrupted , for %+v[prefix=%+v] \n" , key , prefixFlag )    
				        	return
				        }
				        for n, ev := range data.Events {
    						log( "evnet %d : %s ,  key=%+v  new val=%+v  \n", n , ev.Type, ev.Kv.Key, ev.Kv.Value )
    						oldVal := ""
    						if ev.PrevKv!=nil{
    							oldVal=string(ev.PrevKv.Value)
    						}
    						caller( ev.Type , string(ev.Kv.Key) ,  string(ev.Kv.Value) , oldVal )
				        }
    				}else{
    					log( "end watching  ,watch channel was closed , for %+v[prefix=%+v] \n" , key , prefixFlag)    
    					return
    				}
    		}
    	}
	}(c)

	return ch_stop , nil 
}



//=====================================================

type ClientKeepaliveChan = <-chan *clientv3.LeaseKeepAliveResponse

type LeaseInfo struct{
    Lease_id int64    
    Ch_alive_status ClientKeepaliveChan
}

func (c *Client) PutWithLease( keyMap map[string]string , ttl int64 ) ( ch_delete_lease chan bool , leaseInfo LeaseInfo , erro error )  {

    if c.cli == nil {
        return nil , LeaseInfo{} , fmt.Errorf( "CLient has not connected to the server" )
    }

    if len(keyMap)==0 {
    	return nil , LeaseInfo{}   , fmt.Errorf( "input keyMap is empty " )
    }

    if ttl <= 0 {
    	return nil , LeaseInfo{}   , fmt.Errorf( "input ttl=%d is wrong " , ttl )
    }

    log( "PutWithLease,  keyMap=%v , ttl=%d \n" , keyMap  ,ttl )

    ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)         
    resp, err := c.cli.Grant( ctx , ttl )
    cancel()
    if err != nil {
    	log( "failed to watch \n" )    
        log( "%v" , err)
    	return nil   ,LeaseInfo{}  , err
    }
    log( "succeeded to create a lease with id=%d \n" , resp.ID )

    // 会产生一个后端进程，会一直 保持该 lease id 被keepalive
    keep_ctx, keep_cancel := context.WithCancel(context.Background())
    ch, kaerr := c.cli.KeepAlive( keep_ctx , resp.ID)
    if kaerr != nil {
    	return nil  , LeaseInfo{}  , kaerr
    }

    for key , val := range keyMap {
    	if  err:=c.Put( key , val , resp.ID ) ; err != nil  {
    		return nil , LeaseInfo{}  ,  fmt.Errorf( "failed to put %s=%s with the lease id=%d \n" , key , val , resp.ID  ) 
    	}
        log( "succeeded to put %s=%s with the lease id=%d \n" , key , val , resp.ID )  
    }

    ch_close := make(chan bool)
    go func(){
    	<-ch_close
    	log( "receive signal to delete lease id=%+v \n" , resp.ID )
    	keep_cancel()
    	if err:= c.deleteLease(resp.ID) ; err!=nil {
    		log( "failed to delete lease id=%+v \n" , resp.ID )
            return 
    	}
    	log( "succeeded to delete lease id=%+v \n" , resp.ID )
    }()

    return  ch_close , LeaseInfo{ Lease_id: int64(resp.ID) , Ch_alive_status:ch } , nil 
}



func (c *Client) deleteLease(lease_id clientv3.LeaseID ) error  {
    log( "deleteLease  id=%+v \n" , lease_id  )

    if c.cli == nil {
    	return fmt.Errorf( "CLient has not connected to the server" )
    }

    ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)         
    _, err := c.cli.Revoke( ctx ,   lease_id )
    cancel()
    if err != nil {
    	return err
    }

    log( "succeeded to deleteLease id=%+v \n" , lease_id )
    return nil
}




//=====================================================


func (c *Client) TryLock( lockName string  , acquire_seconds_timeout int  ) (ch_unlock  , wait_finish_closing chan bool , er error  ) {
    log( "lock for %+v \n" , lockName  )

    if c.cli == nil {
        return nil , nil , fmt.Errorf( "CLient has not connected to the server" )
    }

    if len(lockName) == 0 {
        return nil  , nil , fmt.Errorf( "miss lockName" )    
    }
    if acquire_seconds_timeout<0 {
        return nil   , nil , fmt.Errorf( "erro acquire_seconds_timeout=%d " , acquire_seconds_timeout )  
    }

    succeed_flag:=make(chan bool)
    ch_unlock=make(chan bool)
    wait_finish_closing=make(chan bool)

    go func(){
        defer func(){
            succeed_flag<-false
        }()
        
        // create two separate sessions for lock competition
        new_session , err := concurrency.NewSession( c.cli )
        if err != nil {
            log( "failed to create session for lock  %+v \n" , lockName  )    
            log( "%v \n" , err)
            return
        }
        defer new_session.Close()
        mutex_lock := concurrency.NewMutex( new_session , lockName )

        var ctx context.Context
        if acquire_seconds_timeout>0 {
            ctx, _ = context.WithTimeout(context.Background() , time.Duration(acquire_seconds_timeout) * time.Second )
            log( "wait %d seconds for acquire lock=%+v \n" , acquire_seconds_timeout ,  lockName  )    
        }else{
            ctx, _ = context.WithCancel(context.Background())
            log("wait  always for acquiring lock=%+v \n"  ,  lockName  )    
        }

        // acquire lock 
        log( "waiting for lock %+v  with wait mode \n" , lockName  )
        if err := mutex_lock.Lock( ctx ); err != nil {
            log( "failed to get lock %+v \n" , lockName  )    
            log( "%v" , err)
            return 
        }
        succeed_flag<-true
        log( "succeeded to lock %+v \n" , lockName  )

        //wait for unlocking
        <-ch_unlock
        log( "try to unlock %+v \n" , lockName  ) 
        defer close(wait_finish_closing)

        if err := mutex_lock.Unlock( context.TODO() ); err != nil {
            log( "failed to unlock %+v \n" , lockName  )    
            log( "%v" , err)
            return
        }
        log( "succeeded to unlock %+v \n" , lockName  )        
        
    }()

    if val , ok := <-succeed_flag ; !ok || !val {
        return nil , nil , fmt.Errorf( "failed to lock %s " , lockName  )  
    }    

    return ch_unlock , wait_finish_closing , nil 
}




//=====================================================


func (c *Client) ElectLeader( topic  , myName string , acquire_seconds_timeout int ) ( ch_close , wait_finish_closing chan bool , er error ) {
    log( "Elect for %+v with name %+v \n" , topic , myName )

    if c.cli == nil {
        return nil , nil , fmt.Errorf( "CLient has not connected to the server" )
    }

    if len(topic) == 0 {
        return nil , nil , fmt.Errorf( "miss topic" )
    }
    if len(myName) == 0 {
        return nil , nil , fmt.Errorf( "miss myName" )
    }
    if acquire_seconds_timeout<0 {
        return nil , nil , fmt.Errorf( "erro acquire_seconds_timeout=%d " , acquire_seconds_timeout  )
    }


    succeed_flag:=make(chan bool)
    ch_close=make(chan bool)
    wait_finish_closing=make(chan bool)

    go func(){
        defer func(){
            succeed_flag<-false
        }()

        session_new, err := concurrency.NewSession( c.cli )
        if err != nil {
            log( "failed to create session for  %+v \n" , topic  )    
            log( "%v" , err)
            return
        }
        defer session_new.Close()

        elect := concurrency.NewElection( session_new , topic )

        var ctx context.Context
        if acquire_seconds_timeout>0 {
            ctx, _ = context.WithTimeout(context.Background() , time.Duration(acquire_seconds_timeout) * time.Second )
            log( "wait %d seconds for acquiring the leader of topic=%+v \n" , acquire_seconds_timeout ,  topic  )    
        }else{
            ctx, _ = context.WithCancel(context.Background())
            log( "wait  always for acquiring the leader of topic=%+v \n"  ,  topic  )    
        }

        if err := elect.Campaign( ctx , myName ); err != nil {
            log( "failed to Campaign for  %+v \n" , topic  )    
            log( "%v" , err)
            return
        }
        succeed_flag<-true
        log( "succeeded to Elect for %+v with name %+v \n" , topic , myName )

        //wait for close
        <-ch_close
        log( "receive signal to stop Elect for %+v with name %+v \n" , topic , myName )
        defer close(wait_finish_closing )

        if err := elect.Resign(context.TODO()); err != nil {
            log( "%v" , err)
            return 
        }

        log( "succeeded to resign the leader of  topic=%+v with name %+v \n" , topic , myName )

    }()

    if val , ok := <-succeed_flag ; !ok || !val {
        return nil, nil , fmt.Errorf( "failed to compain" )
    }
    
    return ch_close , wait_finish_closing , nil 
}




func (c *Client) GetElectLeader( topic string  ) (leader string  , er error ) {
    log( "GetElectLeader for %+v \n" , topic  )

    if c.cli == nil {
        return ""  , fmt.Errorf( "CLient has not connected to the server" )
    }

    if len(topic) == 0 {
        return ""  , fmt.Errorf( "miss topic" )
    }

	session_new, err := concurrency.NewSession( c.cli )
	if err != nil {
    	return "" , fmt.Errorf(  "failed to create session for  %+v , info=%v " , topic ,err  ) 
	}
	defer session_new.Close()
	elect := concurrency.NewElection( session_new , topic )


    ctx, _ := context.WithTimeout(context.Background(), 2 * time.Second )
    if ch := elect.Observe( ctx ) ; ch!=nil {
    	if v , ok := <- ch ; ok {
    		//leader=string(v.Kvs[0].Value)
		    //log( "GetElectLeader found a leader %+v for %+v \n" , leader , topic  )
		    for n , v := range v.Kvs {
		    	log( "for topic=%s , leader=%d , GetElectLeader found %d a leader %+v  \n" , topic , string(v.Value) ,  n  )
		    	leader=string(v.Value)
		    }
    	}else{
		    return "" , fmt.Errorf("there is no leader for %+v " , topic   )
    	}
    }else{
            return "" , fmt.Errorf( "failed to create Observe for %+v"  , topic   )
    }

	return leader , nil
}

//=====================================================

var (
    TxnPutKey="TxnPutKey"
    TxnDelKey="TxnDelKey"
    TxnDelKeyWithPrefix="TxnDelKeyWithPrefix"
)

var (
  // TxnOpPut = clientv3.OpPut
  // TxnOpGet = clientv3.OpGet
  // TxnOpDelete = clientv3.OpDelete
  TxnCompare = clientv3.Compare
  TxnValue = clientv3.Value
)


type TxnCmpStruct = clientv3.Cmp



//check avaliability of op keys
//key不能为空
//所有op的key不能重复 
func convertOpSlice( OpsList []interface{} ) ( []clientv3.Op , error ) {

    if OpsList==nil {
        return []clientv3.Op {} , nil 
    }

    allKey:=[]string{}
    result := []clientv3.Op {}


    for _, item_list := range OpsList {
        if op_list , ok := item_list.( []string ) ; ok {
            if len(op_list)<2 {
                return nil , fmt.Errorf("error, no enough parameter to convertOpSlice "  )
            }
            switch op_list[0] {
            case TxnPutKey :
                    log("put op: %v \n" ,op_list  )
                    if len(op_list)!=3{
                        return nil , fmt.Errorf("error, wrong number of variable for put op"  )
                    }
                    key:=op_list[1]
                    value:=op_list[2]
                    if len(key)==0{
                        return nil , fmt.Errorf("error, empty key string for the key of put op , %T \n ", op_list[1] )
                    }else{
                        for _ , y := range allKey {
                            if strings.Contains(y , key ) || strings.Contains(y , key ) {
                                return nil , fmt.Errorf("error, forbid to op two same key, key=%s \n ", key )
                            }
                        }
                        allKey=append(allKey , key )
                        result=append( result, clientv3.OpPut( key , value ) )
                    }
                    

            case TxnDelKey , TxnDelKeyWithPrefix :
                    log("delete op: %v \n" , op_list )
                    if len(op_list)!=2 {
                        return nil , fmt.Errorf("error, wrong number of variable for delete op")
                    }
                    key:=op_list[1]
                    if len(key)==0{
                        return nil , fmt.Errorf("error, empty string for the key of delete op , %T \n ", op_list[1] )
                    }else{
                        for _ , y := range allKey {
                            if strings.Contains(y , key ) || strings.Contains(y , key ) {
                                return nil , fmt.Errorf("error, forbid to op two same key, key=%s \n ", key )
                            }
                        }                            
                        allKey=append(allKey , key )
                        if op_list[0]==TxnDelKeyWithPrefix{
                            result=append( result ,clientv3.OpDelete( key , clientv3.WithPrefix() ) )
                        }else{
                            result=append( result ,clientv3.OpDelete( key ) )
                        }
                    }
                    
            default:
                fmt.Printf("error op  , %v \n" , op_list[0] )
            } 

        }else{
            return nil , fmt.Errorf("error op list , %v \n" , op_list)    
        }
    }
    return  result , nil 

}



func (c *Client) TxnExecCmpValue( valueCmp []TxnCmpStruct , thenOpsList []interface{}  , elseOpsList []interface{} ) ( ifIsTrue bool , er error ) {

    if c.cli == nil {
        return  false , fmt.Errorf( "CLient has not connected to the server" )
    }

    if thenOpsList==nil && elseOpsList==nil {
        return false, fmt.Errorf( "both if and else is nill" )

    }

    thenOpsRe , erra :=convertOpSlice( thenOpsList )
    if erra!=nil{
        return false , erra
    }
    elseOpsRe , errb :=convertOpSlice( elseOpsList )
    if errb !=nil{
        return false , errb 
    }

    kvc :=clientv3.NewKV(c.cli)

    log( "valueCmp: %+v \n" , valueCmp)    
    log( "thenOps: %+v \n" , thenOpsRe)    
    log( "ElseOps: %+v \n" , elseOpsRe)    

    ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second )
    result , err := kvc.Txn( ctx ).If( valueCmp... ).Then( thenOpsRe...  ).Else( elseOpsRe...).Commit()  
    cancel()
    if err != nil {
        return false , err
    }

    log( "%+v  \n"   , result )
    if result.Succeeded {
        log( "if is true , execute then commands  \n"   , result )
        return true, nil 
    }else{
        log( "if is false , execute else commands  \n"   , result )
        return false, nil
    }

}




func (c *Client) TxnExec( ops []interface{} ) error  {

    if c.cli == nil {
        return  fmt.Errorf( "CLient has not connected to the server" )
    }
    if ops == nil || len(ops)==0  {
        return  fmt.Errorf( "no inputted op" )
    }
    thenOpsRe , erra :=convertOpSlice( ops )
    if erra!=nil{
        return  erra
    }


    kvc :=clientv3.NewKV(c.cli)

    log( "ops: %+v \n" , thenOpsRe)    

    ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second )
    result , err := kvc.Txn( ctx ).Then( thenOpsRe...  ).Commit()  
    cancel()
    if err != nil {
        return err 
    }

    log( "txn succeeded  \n"   )
    log( "%+v  \n"   , result )

    return nil
}

//=====================================================


func (c *Client) Close() {
	if c.cli != nil {
		c.cli.Close()
	}
}



