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
    clog "github.com/weizhouBlue/go_log"
)


type ClientKeepaliveChan = <-chan *clientv3.LeaseKeepAliveResponse


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

//=====================================================


func (c *Client) Connect( endpoints []string  ) bool {

	var tlsConfig *tls.Config
    var etcd_endpoints []string
    var etcd_ca string
    var etcd_key string
    var etcd_cert string

	if len(endpoints) ==0 && len(Global_endpoints)==0  {
    	clog.Log(clog.Err ," no input of etcd server "  )
		return false

	}else if len(endpoints) ==0 && len(Global_endpoints)!=0  {
        clog.Log(clog.Debug ," use gloabl endpoints "  )
        etcd_endpoints=Global_endpoints
        etcd_ca=Global_Tls_ca
        etcd_key=Global_Tls_key
        etcd_cert=Global_Tls_cert

    }else if len(endpoints) !=0 {
        clog.Log(clog.Debug ," use instance endpoints "  )
        etcd_endpoints=endpoints
        etcd_ca=c.Tls_ca
        etcd_key=c.Tls_key
        etcd_cert=c.Tls_cert
    } 

    clog.Log(clog.Info , "etcd client try to connect to server %v " , etcd_endpoints  )


	if strings.Contains( etcd_endpoints[0] , "https" ) {
		if existFile( etcd_ca )==false   {
    		clog.Log(clog.Err ,"error, no file cert-ca "  )
			return false
		}
		if existFile( etcd_key )==false   {
    		clog.Log(clog.Err ,"error, no file cert-key "  )
			return false
		}
		if existFile( etcd_cert )==false   {
    		clog.Log(clog.Err ,"error, no file cert-cert "  )
			return false
		}

	    tlsInfo := transport.TLSInfo{
	        CertFile:       etcd_cert ,
	        KeyFile:        etcd_key  ,
	        TrustedCAFile:  etcd_ca ,
	    }
	    info, err := tlsInfo.ClientConfig()
	    if err != nil {
    		clog.Log(clog.Err , " %v " , err )
	        return false
	    }
	    tlsConfig=info
    	clog.Log(clog.Debug ,"cert files are available "  )

	}

    cli, err := clientv3.New(clientv3.Config{
        Endpoints:   etcd_endpoints ,
        DialTimeout: dialTimeout ,
        TLS:         tlsConfig ,
    })
    if err != nil {
    	clog.Log(clog.Err ,"error, failed to connect to the server "  )
        return false
    }
	c.cli=cli

    clog.Log(clog.Info ,"succeeded to create to the etcd server" )
    return true
}



func (c *Client) Put( key string , value string  , lease_id ... clientv3.LeaseID ) bool {

    if c.cli == nil {
    	clog.Log( clog.Err , "CLient has not connect to the server \n" )    
    	return false
    }

    if len(key)==0 {
        clog.Log( clog.Err , "error, key is empty \n" )    
        return false
    }

    var err error
    ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
    if len(lease_id) > 0 && int64(lease_id[0]) > 0 {
	    clog.Log(clog.Debug, "put etcd  %s=%s with lease id=%v \n" , key ,value , lease_id[0] )
    	_, err = c.cli.Put(ctx , key , value, clientv3.WithLease( lease_id[0] ) )

    }else{
	    clog.Log(clog.Debug, "put etcd  %s=%s \n" , key ,value  )
    	_, err = c.cli.Put( ctx , key , value )
    }
    cancel()
    if err != nil {
        switch err {
	        case context.Canceled:
    			clog.Log(clog.Err ,   "ctx is canceled by another routine: %v\n", err)
	        case context.DeadlineExceeded:
	            clog.Log(clog.Err ,  "ctx is attached with a deadline is exceeded: %v\n", err)
	        case rpctypes.ErrEmptyKey:
	            clog.Log(clog.Err ,  "client-side error: %v\n", err)
	        default:
	            clog.Log(clog.Err ,  "bad cluster endpoints, which are not etcd servers: %v\n", err)
        }
		clog.Log(clog.Err , "failed to put , %s=%s \n" , key , value)      
        return false
    }
    
    clog.Log( clog.Debug , "succeeded to put , %s=%s \n" , key , value)    
    return true
}



func (c *Client) Get( key string) (string , bool) {
    clog.Log(clog.Debug, "get etcd  key=%s \n" , key  )

    if c.cli == nil {
    	clog.Log( clog.Err , "CLient has not connect to the server \n" )    
    	return "" , false
    }

    if len(key)==0 {
        clog.Log( clog.Err , "error, key is empty \n" )    
        return "" , false 
    }


    ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
    resp, err := c.cli.Get(ctx, key )
    cancel()
    clog.Log(clog.Info, "response=%v \n" , resp)

    if err != nil  {
        clog.Log(clog.Err, "failed to get etcd key=%s \n" , key)
        clog.Log(clog.Err , "%v" , err)
        return "",false
    }


    if len(resp.Kvs)==0 {
        return "" , true
    }else{
        result := resp.Kvs[0].Value     
        return string(result) , true
    }

}



func (c *Client) GetPrefix( prefix string) ( map[string]string  , bool ) {
    clog.Log( clog.Debug, "GetPrefix etcd  prefix=%s \n" , prefix  )

    if c.cli == nil {
    	clog.Log( clog.Err , "CLient has not connect to the server \n" )    
    	return nil , false
    }

    if len(prefix)==0 {
        clog.Log( clog.Err , "error, prefix is empty \n" )    
        return nil , false
    }


    ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
    resp, err := c.cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
    cancel()
    clog.Log(clog.Info, "response=%v \n" , resp)

    if err != nil {
        clog.Log(clog.Err, "failed to get etcd key=%s \n" , prefix)
        clog.Log(clog.Err , "%v" , err)
        return nil , false
    }
    if len(resp.Kvs)==0 {
        return nil , true
    }

    result := make( map[string] string ) 
    for _, ev := range resp.Kvs {
        clog.Log( clog.Debug , "GetPrefix get %s : %s  reversion=%d \n  ", ev.Key, ev.Value , resp.Header.Revision )
        result[ string(ev.Key) ]= string(ev.Value)
    }
    clog.Log(clog.Debug, "succeeded to GetPrefix %v \n"  , result )
    return result , true
}



func (c *Client) Delete( key string , prefixFlag bool  ) bool {
    clog.Log( clog.Debug, "delete etcd  key=%s \n" , key  )

    if c.cli == nil {
    	clog.Log( clog.Err , "CLient has not connect to the server \n" )    
    	return false
    }


    if len(key)==0 {
        clog.Log( clog.Err , "error, key is empty \n" )    
        return false
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
        clog.Log(clog.Err, "failed to delete etcd key=%s \n" , key)
        clog.Log(clog.Err , "%v" , err)
        return false
    }
    clog.Log(clog.Debug, "succeeded to delete key=%v \n"  , key )
    return true
}




// type ClientWatchChan = clientv3.WatchChan

// func (c *Client) Watch( key string , prefixFlag bool )  ClientWatchChan {
//     clog.Log( clog.Debug, "Watch etcd  key=%s \n" , key  )

//     if c.cli == nil {
//     	clog.Log( clog.Err , "CLient has not connect to the server \n" )    
//     	return nil
//     }

//     if prefixFlag {
//     	return c.cli.Watch(context.Background(), key , clientv3.WithPrefix())
//     }else{
//     	return c.cli.Watch( context.Background() , key )
//     }
// }


//=====================================================


func (c *Client) WatchByHandler( key string , prefixFlag bool ,	caller func(evnet EventWatch , key , newVal , oldVal string ) )  (ch_stop chan bool ){
    clog.Log( clog.Debug, "Watch etcd  key=%s \n" , key  )

    if c.cli == nil {
    	clog.Log( clog.Err , "CLient has not connect to the server \n" )    
    	return nil
    }

    if len(key)==0 {
        clog.Log( clog.Err , "error, key is empty \n" )    
        return nil
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
    				clog.Log( clog.Debug , "end watching , receive signal to stop watch, for %q[prefix=%q] \n" , key , prefixFlag)    
    				c.cli.Watcher.Close()
    				return 

    			case data , ok := <- eventChan :
    				if ok {
				        if data.Canceled {
    						clog.Log( clog.Err , "end watching , Watch was interrupted , for %q[prefix=%q] \n" , key , prefixFlag )    
				        	return
				        }
				        for n, ev := range data.Events {
    						clog.Log( clog.Debug , "evnet %d : %s ,  key=%q  new val=%q  \n", n , ev.Type, ev.Kv.Key, ev.Kv.Value )
    						oldVal := ""
    						if ev.PrevKv!=nil{
    							oldVal=string(ev.PrevKv.Value)
    						}
    						caller( ev.Type , string(ev.Kv.Key) ,  string(ev.Kv.Value) , oldVal )
				        }
    				}else{
    					clog.Log( clog.Err , "end watching  ,watch channel was closed , for %q[prefix=%q] \n" , key , prefixFlag)    
    					return
    				}
    		}
    	}
	}(c)

	return ch_stop
}



//=====================================================


func (c *Client) PutWithLease( keyMap map[string]string , ttl int64 ) ( ch_alive_status ClientKeepaliveChan , lease_id int64 , ch_delete_lease chan bool)  {

    if c.cli == nil {
    	clog.Log( clog.Err , "CLient has not connected to the server \n" )    
    	return nil , 0 , nil
    }

    if len(keyMap)==0 {
    	clog.Log( clog.Err , "input keyMap is empty \n" )    
    	return nil , 0 , nil
    }

    if ttl <= 0 {
    	clog.Log( clog.Err , "input ttl=%d is wrong \n" , ttl)    
    	return nil , 0 , nil
    }

    clog.Log( clog.Debug, "PutWithLease,  keyMap=%v , ttl=%d \n" , keyMap  ,ttl )

    ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)         
    resp, err := c.cli.Grant( ctx , ttl )
    cancel()
    if err != nil {
    	clog.Log( clog.Err , "failed to watch \n" )    
        clog.Log( clog.Err , "%v" , err)
    	return nil , 0  ,nil
    }
    clog.Log( clog.Debug, "succeeded to create a lease with id=%d \n" , resp.ID )

    // 会产生一个后端进程，会一直 保持该 lease id 被keepalive
    keep_ctx, keep_cancel := context.WithCancel(context.Background())
    ch, kaerr := c.cli.KeepAlive( keep_ctx , resp.ID)
    if kaerr != nil {
    	clog.Log( clog.Err , "failed to keepalive the lease id=%d \n" , resp.ID )    
        clog.Log( clog.Err , "%v" , err)
    	return nil , 0  , nil
    }

    for key , val := range keyMap {
    	if ! c.Put( key , val , resp.ID ) {
    		clog.Log( clog.Err , "failed to put %s=%s with the lease id=%d \n" , key , val , resp.ID )  
    		return nil , 0  , nil
    	}
    	clog.Log( clog.Debug , "succeeded to put %s=%s with the lease id=%d \n" , key , val , resp.ID )  
    }

    ch_close := make(chan bool)
    go func(){
    	<-ch_close
    	clog.Log(clog.Debug, "receive signal to delete lease id=%q \n" , resp.ID )
    	keep_cancel()
    	if ! c.deleteLease(resp.ID){
    		clog.Log(clog.Err, "failed to delete lease id=%q \n" , resp.ID )
    	}
    	clog.Log(clog.Debug, "succeeded to delete lease id=%q \n" , resp.ID )
    }()

    return ch , int64(resp.ID) , ch_close
}

func (c *Client) deleteLease(lease_id clientv3.LeaseID ) bool {
    clog.Log( clog.Debug, "deleteLease  id=%q \n" , lease_id  )

    if c.cli == nil {
    	clog.Log( clog.Err , "CLient has not connect to the server \n" )    
    	return false
    }

    ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)         
    _, err := c.cli.Revoke( ctx ,   lease_id )
    cancel()
    if err != nil {
    	clog.Log( clog.Err , "failed to deleteLease id=%q \n" , lease_id  )    
        clog.Log( clog.Err , "%v" , err)
    	return false
    }

    clog.Log(clog.Debug, "succeeded to deleteLease id=%q \n" , lease_id )
    return true
}




//=====================================================


func (c *Client) TryLock( lockName string  , acquire_seconds_timeout int  ) (ch_unlock  , wait_finish_closing chan bool ,  ) {
    clog.Log( clog.Debug, "lock for %q \n" , lockName  )

    if c.cli == nil {
        clog.Log( clog.Err , "CLient has not connect to the server \n" )    
        return nil , nil 
    }

    if len(lockName) == 0 {
        clog.Log( clog.Err , "miss lockName \n" )    
        return nil  , nil     
    }
    if acquire_seconds_timeout<0 {
        clog.Log( clog.Err , "erro acquire_seconds_timeout=%d \n" , acquire_seconds_timeout )    
        return nil   , nil 
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
            clog.Log( clog.Err , "failed to create session for lock  %q \n" , lockName  )    
            clog.Log( clog.Err , "%v \n" , err)
            return 
        }
        defer new_session.Close()
        mutex_lock := concurrency.NewMutex( new_session , lockName )

        var ctx context.Context
        if acquire_seconds_timeout>0 {
            ctx, _ = context.WithTimeout(context.Background() , time.Duration(acquire_seconds_timeout) * time.Second )
            clog.Log( clog.Info , "wait %d seconds for acquire lock=%q \n" , acquire_seconds_timeout ,  lockName  )    
        }else{
            ctx, _ = context.WithCancel(context.Background())
            clog.Log( clog.Info , "wait  always for acquiring lock=%q \n"  ,  lockName  )    
        }

        // acquire lock 
        clog.Log( clog.Debug, "waiting for lock %q  with wait mode \n" , lockName  )
        if err := mutex_lock.Lock( ctx ); err != nil {
            clog.Log( clog.Err , "failed to get lock %q \n" , lockName  )    
            clog.Log( clog.Err , "%v" , err)
            return 
        }
        succeed_flag<-true
        clog.Log( clog.Debug, "succeeded to lock %q \n" , lockName  )

        //wait for unlocking
        <-ch_unlock
        clog.Log( clog.Debug , "try to unlock %q \n" , lockName  ) 
        defer close(wait_finish_closing)
        
        if err := mutex_lock.Unlock( context.TODO() ); err != nil {
            clog.Log( clog.Err , "failed to unlock %q \n" , lockName  )    
            clog.Log( clog.Err , "%v" , err)
            return
        }
        clog.Log( clog.Debug , "succeeded to unlock %q \n" , lockName  )        
        
    }()

    if val , ok := <-succeed_flag ; !ok || !val {
        clog.Log( clog.Err , "failed to lock %s \n" , lockName )
        return nil , nil 
    }    

    return ch_unlock , wait_finish_closing
}




//=====================================================


func (c *Client) ElectLeader( topic  , myName string , acquire_seconds_timeout int ) ( ch_close , wait_finish_closing chan bool ) {
    clog.Log( clog.Debug, "Elect for %q with name %q \n" , topic , myName )

    if c.cli == nil {
        clog.Log( clog.Err , "CLient has not connect to the server \n" )    
        return nil , nil
    }

    if len(topic) == 0 {
        clog.Log( clog.Err , "miss topic \n" )    
        return nil      , nil
    }
    if len(myName) == 0 {
        clog.Log( clog.Err , "miss myName \n" )    
        return nil     , nil 
    }
    if acquire_seconds_timeout<0 {
        clog.Log( clog.Err , "erro acquire_seconds_timeout=%d \n" , acquire_seconds_timeout )    
        return nil   , nil
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
            clog.Log( clog.Err , "failed to create session for  %q \n" , topic  )    
            clog.Log( clog.Err , "%v" , err)
            return
        }
        defer session_new.Close()

        elect := concurrency.NewElection( session_new , topic )

        var ctx context.Context
        if acquire_seconds_timeout>0 {
            ctx, _ = context.WithTimeout(context.Background() , time.Duration(acquire_seconds_timeout) * time.Second )
            clog.Log( clog.Info , "wait %d seconds for acquiring the leader of topic=%q \n" , acquire_seconds_timeout ,  topic  )    
        }else{
            ctx, _ = context.WithCancel(context.Background())
            clog.Log( clog.Info , "wait  always for acquiring the leader of topic=%q \n"  ,  topic  )    
        }

        if err := elect.Campaign( ctx , myName ); err != nil {
            clog.Log( clog.Err , "failed to Campaign for  %q \n" , topic  )    
            clog.Log( clog.Err , "%v" , err)
            return
        }
        succeed_flag<-true
        clog.Log( clog.Debug, "succeeded to Elect for %q with name %q \n" , topic , myName )

        //wait for close
        <-ch_close
        clog.Log( clog.Debug, "receive signal to stop Elect for %q with name %q \n" , topic , myName )
        defer close(wait_finish_closing )

        if err := elect.Resign(context.TODO()); err != nil {
            clog.Log( clog.Err , "%v" , err)
            return 
        }

        clog.Log( clog.Debug, "succeeded to resign the leader of  topic=%q with name %q \n" , topic , myName )

    }()

    if val , ok := <-succeed_flag ; !ok || !val {
        clog.Log( clog.Err , "failed to compain" )
        return nil, nil
    }
    
    return ch_close , wait_finish_closing
}




func (c *Client) GetElectLeader( topic string  ) (leader string  , ok bool) {
    clog.Log( clog.Debug, "GetElectLeader for %q \n" , topic  )

    if c.cli == nil {
    	clog.Log( clog.Err , "CLient has not connect to the server \n" )    
    	return "" , false
    }

    if len(topic) == 0 {
    	clog.Log( clog.Err , "miss topic \n" )    
    	return "" , false   	
    }

	session_new, err := concurrency.NewSession( c.cli )
	if err != nil {
    	clog.Log( clog.Err , "failed to create session for  %q \n" , topic  )    
        clog.Log( clog.Err , "%v" , err)
    	return "" , false
	}
	defer session_new.Close()
	elect := concurrency.NewElection( session_new , topic )


    ctx, _ := context.WithTimeout(context.Background(), 2 * time.Second )
    if ch := elect.Observe( ctx ) ; ch!=nil {
    	if v , ok := <- ch ; ok {
    		//leader=string(v.Kvs[0].Value)
		    //clog.Log( clog.Debug, "GetElectLeader found a leader %q for %q \n" , leader , topic  )
		    for n , v := range v.Kvs {
		    	clog.Log( clog.Debug, "for topic=%s , leader=%d , GetElectLeader found %d a leader %q  \n" , topic , string(v.Value) ,  n  )
		    	leader=string(v.Value)
		    }
    	}else{
		    clog.Log( clog.Debug, "there is no leader for %q  \n" , topic   )
		    return "" , true
    	}
    }else{
		clog.Log( clog.Err, "failed to create Observe for %q \n"  , topic  )
		return "" , false
    }



	return leader , true
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

func (c *Client) TxnExec( ops []TxnOpStruct ) bool {

    if c.cli == nil {
        clog.Log( clog.Err , "CLient has not connect to the server \n" )    
        return  false
    }

    kvc :=clientv3.NewKV(c.cli)

    clog.Log( clog.Debug , "ops: %q \n" , ops)    

    ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second )
    result , err := kvc.Txn( ctx ).Then( ops...  ).Commit()  
    cancel()
    if err != nil {
        clog.Log( clog.Err, "txn failed  \n"   )
        clog.Log( clog.Err, "%q  \n" , err  )
        return false
    }

    clog.Log( clog.Info, "txn succeeded  \n"   )
    clog.Log( clog.Debug, "%q  \n"   , result )


    return true
}





type TxnCmpStruct = clientv3.Cmp


func (c *Client) TxnExecCmpValue( valueCmp []TxnCmpStruct , thenOps []TxnOpStruct  , ElseOps []TxnOpStruct ) ( ifOK , execOk bool) {

    if c.cli == nil {
        clog.Log( clog.Err , "CLient has not connect to the server \n" )    
        return  false, false
    }

    kvc :=clientv3.NewKV(c.cli)

    clog.Log( clog.Debug , "valueCmp: %q \n" , valueCmp)    
    clog.Log( clog.Debug , "thenOps: %q \n" , thenOps)    
    clog.Log( clog.Debug , "ElseOps: %q \n" , ElseOps)    

    ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second )
    result , err := kvc.Txn( ctx ).If( valueCmp... ).Then( thenOps...  ).Else( ElseOps...).Commit()  
    cancel()
    if err != nil {
        clog.Log( clog.Err, "txn failed  \n"   )
        clog.Log( clog.Err, "%q  \n" , err  )
        return false , false
    }

    clog.Log( clog.Debug, "%q  \n"   , result )
    if result.Succeeded {
        clog.Log( clog.Info, "if is ok , execute then commands  \n"   , result )
        return true, true 
    }else{
        clog.Log( clog.Info, "if is not ok , execute else commands  \n"   , result )
        return false, true
    }

}





//=====================================================


func (c *Client) Close() {
	if c.cli != nil {
		c.cli.Close()
	}
}



