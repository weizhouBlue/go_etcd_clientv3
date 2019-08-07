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


func (c *Client) Connect( endpoints []string  ) bool {

    clog.Log(clog.Info , "etcd client try to connect to server %v " , endpoints  )


	var tlsConfig *tls.Config

	if len(endpoints) ==0 {
    	clog.Log(clog.Err ," no input of etcd server "  )
		return false
	}

	//if len( c.Tls_ca)!=0 || len( c.Tls_key)!=0 || len( c.Tls_cert)!=0 {
	if strings.Contains( endpoints[0] , "https" ) {
		if existFile( c.Tls_ca)==false   {
    		clog.Log(clog.Err ,"error, no file cert-ca "  )
			return false
		}
		if existFile( c.Tls_key)==false   {
    		clog.Log(clog.Err ,"error, no file cert-key "  )
			return false
		}
		if existFile( c.Tls_cert)==false   {
    		clog.Log(clog.Err ,"error, no file cert-cert "  )
			return false
		}

	    tlsInfo := transport.TLSInfo{
	        CertFile:       c.Tls_cert ,
	        KeyFile:        c.Tls_key ,
	        TrustedCAFile:  c.Tls_ca ,
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
        Endpoints:   endpoints ,
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

    ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
    resp, err := c.cli.Get(ctx, key )
    cancel()
    if err != nil || len(resp.Kvs)==0 {
        clog.Log(clog.Err, "failed to get etcd key=%s \n" , key)
        clog.Log(clog.Err , "%v" , err)
        return "",false
    }
   	result := resp.Kvs[0].Value     
    clog.Log(clog.Debug, "succeeded to get %s=%s \n" , key , result )
    return string(result) , true
}



func (c *Client) GetPrefix( prefix string) map[string]string  {
    clog.Log( clog.Debug, "GetPrefix etcd  prefix=%s \n" , prefix  )

    if c.cli == nil {
    	clog.Log( clog.Err , "CLient has not connect to the server \n" )    
    	return nil
    }

    ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
    resp, err := c.cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
    cancel()
    if err != nil || len(resp.Kvs)==0 {
        clog.Log(clog.Err, "failed to get etcd key=%s \n" , prefix)
        clog.Log(clog.Err , "%v" , err)
        return nil
    }

    result := make( map[string] string ) 
    for _, ev := range resp.Kvs {
        clog.Log( clog.Debug , "GetPrefix get %s : %s  reversion=%d \n  ", ev.Key, ev.Value , resp.Header.Revision )
        result[ string(ev.Key) ]= string(ev.Value)
    }
    clog.Log(clog.Debug, "succeeded to GetPrefix %v \n"  , result )
    return result
}



func (c *Client) Delete( key string , prefixFlag bool  ) bool {
    clog.Log( clog.Debug, "delete etcd  key=%s \n" , key  )

    if c.cli == nil {
    	clog.Log( clog.Err , "CLient has not connect to the server \n" )    
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



func (c *Client) WatchByHandler( key string , prefixFlag bool ,	caller func(evnet EventWatch , key , newVal , oldVal string ) )  (ch_stop chan bool ){
    clog.Log( clog.Debug, "Watch etcd  key=%s \n" , key  )

    if c.cli == nil {
    	clog.Log( clog.Err , "CLient has not connect to the server \n" )    
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
    ch, kaerr := c.cli.KeepAlive( context.TODO() , resp.ID)
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



func (c *Client) Lock( lockName string ) (ch_unlock chan bool) {
    clog.Log( clog.Debug, "lock for %q \n" , lockName  )

    if c.cli == nil {
    	clog.Log( clog.Err , "CLient has not connect to the server \n" )    
    	return nil
    }

    if len(lockName) == 0 {
    	clog.Log( clog.Err , "miss lockName \n" )    
    	return nil    	
    }

    // create two separate sessions for lock competition
    new_session , err := concurrency.NewSession( c.cli )
    if err != nil {
    	clog.Log( clog.Err , "failed to create session for  %q \n" , lockName  )    
        clog.Log( clog.Err , "%v" , err)
    	return nil
    }
    mutex_lock := concurrency.NewMutex( new_session , lockName )

    // acquire lock 
    clog.Log( clog.Debug, "waiting for lock %q \n" , lockName  )    
    if err := mutex_lock.Lock(context.TODO()); err != nil {
    	clog.Log( clog.Err , "failed to create session for  %q \n" , lockName  )    
        clog.Log( clog.Err , "%v" , err)
        new_session.Close()
    	return nil
    }
    clog.Log( clog.Debug , "acquired lock %q \n" , lockName  ) 

    ch_unlock = make(chan bool)
    go func(){
    	<-ch_unlock
    	clog.Log( clog.Debug , "begin to unlock %q \n" , lockName  ) 
    	new_session.Close()
	    if err := mutex_lock.Unlock(context.TODO()); err != nil {
	    	clog.Log( clog.Err , "failed to unlock %q \n" , lockName  )    
	        clog.Log( clog.Err , "%v" , err)
    		return
	    }
    	clog.Log( clog.Debug , "succeeded to unlock %q \n" , lockName  ) 	    
    }()

    return ch_unlock
}


func (c *Client) Close() {
	if c.cli != nil {
		c.cli.Close()
	}
}



