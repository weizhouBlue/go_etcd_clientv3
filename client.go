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
    "reflect"
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


func (c *Client) WatchByHandler( key string , prefixFlag bool ,	caller func(evnet EventWatch , key , newVal , oldVal string ) )  (ch_stop chan bool , err error ){
    log( "Watch etcd  key=%s \n" , key  )

    if c.cli == nil {
    	return nil ,  fmt.Errorf("CLient has not connect to the server " )  
    }

    if len(key)==0 {
        return nil , fmt.Errorf( "error, key is empty " ) 
    }

    var eventChan clientv3.WatchChan
    if prefixFlag {
    	eventChan = c.cli.Watch(context.Background(), key , clientv3.WithPrefix())
    }else{
    	eventChan = c.cli.Watch( context.Background() , key )
    }

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
  TxnOpPut = clientv3.OpPut
  TxnOpGet = clientv3.OpGet
  TxnOpDelete = clientv3.OpDelete
  TxnCompare = clientv3.Compare
  Value = clientv3.Value
)

type TxnOpStruct = clientv3.Op
type TxnCmpStruct = clientv3.Cmp



//check avaliability of op keys
//key不能为空
//所有op的key不能重复 
func convertOpSlice( OpsList []interface{} ) ( []TxnOpStruct , error ) {


    if OpsList==nil {
        return nil , nil 
    }

    allKey:=[]string{}
    result := []TxnOpStruct {}


    for _, item_list := range OpsList {
        if op_list , ok := item_list.( []interface{} ) ; ok {

            switch op_list[0].(type){
            case func( string, string,  ...clientv3.OpOption ) clientv3.Op :
                fun , _ := op_list[0].(func( string, string,  ...clientv3.OpOption )clientv3.Op  )
                if reflect.ValueOf(fun).Pointer()  ==  reflect.ValueOf(TxnOpPut).Pointer()  {
                    log("put op")
                    if len(op_list)!=3{
                        return nil , fmt.Errorf("error, wrong number of variable for put op"  )
                    }
                    value , value_ok := op_list[2].(string)
                    if  value_ok==false {
                        return nil , fmt.Errorf("error, wrong var type of the value of put op , %T \n ", op_list[1] )
                    }

                    key , key_ok := op_list[1].(string)
                    if  key_ok==false {
                        return nil , fmt.Errorf("error, wrong var type of the key of put op , %T \n ", op_list[1] )
                    }else{
                        if len(key)==0{
                            return nil , fmt.Errorf("error, empty string for the key of put op , %T \n ", op_list[1] )
                        }else{
                            for _ , y := range allKey {
                                if y==key {
                                    return nil , fmt.Errorf("error, forbid to op two same key, key=%s \n ", key )
                                }
                            }
                            allKey=append(allKey , key )
                            result=append( result, TxnOpPut( key , value ) )
                        }
                    }
                    
                }else{
                    return nil , fmt.Errorf("error, unsupported op  ")
                }
            case func( string,  ...clientv3.OpOption )clientv3.Op :
                fun , _ := op_list[0].(func( string ,  ...clientv3.OpOption )clientv3.Op )
                if reflect.ValueOf(fun).Pointer()  ==  reflect.ValueOf( TxnOpDelete ).Pointer()  {
                    log("delete op")
                    if len(op_list)!=2 {
                        return nil , fmt.Errorf("error, wrong number of variable for delete op")
                    }
                    key , key_ok := op_list[1].(string)
                    if  key_ok==false {
                        return nil , fmt.Errorf("error, wrong var type of the key of put op , %T \n ", op_list[1] )
                    }else{
                        if len(key)==0{
                            return nil , fmt.Errorf("error, empty string for the key of delete op , %T \n ", op_list[1] )
                        }else{
                            for _ , y := range allKey {
                                if y==key {
                                    return nil , fmt.Errorf("error, forbid to op two same key, key=%s \n ", key )
                                }
                            }                            
                            allKey=append(allKey , key )
                            result=append( result ,TxnOpDelete( key ) )
                        }
                    }

                }else{
                    return nil , fmt.Errorf("error, unsupported op")
                }

            default:
                fmt.Printf("error item list  , %T \n" , op_list[0] )
            } 

        }else{
            return nil , fmt.Errorf("error item , %v \n" , op_list)    
        }
    }
    return  result , nil 

}



func (c *Client) TxnExecCmpValue( valueCmp []TxnCmpStruct , thenOpsList []interface{}  , elseOpsList []interface{} ) ( ifIsTrue bool , er error ) {

    if c.cli == nil {
        return  false , fmt.Errorf( "CLient has not connected to the server" )
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



