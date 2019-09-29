package go_etcd_clientv3_test
import (
	"testing"
	etcd "github.com/weizhouBlue/go_etcd_clientv3"
	"fmt"
    log "github.com/weizhouBlue/go_log"
    "time"
)

//====================================

func Test_instance_config(t *testing.T){
	log.Config(  log.Debug , " test module" , "" )  

	var c etcd.Client

	c = etcd.Client {
		Tls_cert : "./test-tls-cert/etcd-cert" ,
		Tls_ca	: "./test-tls-cert/etcd-ca" ,
		Tls_key : "./test-tls-cert/etcd-key" ,
	}

	if ! c.Connect( []string {"https://10.6.185.150:12379" } ) {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()
}



func Test_GloablConfig(t *testing.T){
	log.Config(  log.Debug , " test global module" , "" )  

	etcd.Global_Tls_ca="./test-tls-cert/etcd-ca"
	etcd.Global_Tls_cert="./test-tls-cert/etcd-cert"
	etcd.Global_Tls_key="./test-tls-cert/etcd-key"
	etcd.Global_endpoints=[]string {"https://10.6.185.150:12379" }

	c:= etcd.Client{}
	if ! c.Connect( nil ) {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()
}



//====================================

func Test_basic(t *testing.T){
	log.Config(  log.Debug , " test module" , "" )  

	var c etcd.Client
	var ok bool
	var val string

	if ! c.Connect( []string {"http://127.0.0.1:2379" } ) {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()



	val , ok = c.Get("/a20" )
	if !ok {
		fmt.Println("failed to get ")
		t.FailNow()		
	}
	if len(val)==0 {
		fmt.Printf("key is empty \n" )
	}else{
		fmt.Printf("succeeded to get %s \n" , val )
	}



	if v , ok:= c.GetPrefix("/bbbb") ; ok {
		if v==nil {
			fmt.Printf("prefix is empty  \n" )
		}else{
			fmt.Printf("succeeded to GetPrefix %v \n" , v )
		}
	}else{
		fmt.Println("failed to GetPrefix ")
		t.FailNow()			
	}



	if ! c.Put("/a1" , "100") {
		fmt.Println("failed to put etcd")
		t.FailNow()		
	}
	if ! c.Put("/a2" , "200") {
		fmt.Println("failed to put etcd")
		t.FailNow()		
	}
	fmt.Println("succeeded to put etcd")



	val , ok = c.Get("/a1" )
	if !ok {
		fmt.Println("failed to get ")
		t.FailNow()		
	}
	if len(val)==0 {
		fmt.Printf("key is empty \n" )
	}else{
		fmt.Printf("succeeded to get %s \n" , val )
	}



	if v , ok:= c.GetPrefix("/a") ; ok {
		if v==nil {
			fmt.Printf("prefix is empty  \n" )
		}else{
			fmt.Printf("succeeded to GetPrefix %v \n" , v )
		}
	}else{
		fmt.Println("failed to GetPrefix ")
		t.FailNow()			
	}



	if  c.Delete("/a10" , false ) {
		fmt.Printf("succeeded to delete a10 \n" )
	}else{
		fmt.Println("failed to delete ")
		t.FailNow()			
	}

}




//====================================

func Test_lease(t *testing.T){
	log.Config(  log.Debug , " test module" , "" )  
	var c etcd.Client


	if ! c.Connect( []string {"http://127.0.0.1:2379" } ) {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()

	if ch , lease_id , ch_delete := c.PutWithLease( map[string]string{"la":"200" , "lb":"201"} , 5 ) ; ch==nil{
		fmt.Println("failed to PutWithLease ")
		t.FailNow()			
	}else{
		fmt.Printf( "succeeded to PutWithLease id=%q " , lease_id )

	    go func(){
	    	for v  := range ch {
	    			log.Log( log.Info , "sent a keepalive , message =%q  \n" , v )    
	    	}
	    	log.Log( log.Err , "keepalived was interrupted\n"  )    
	    }()

	    time.Sleep(15*time.Second)

	    close(ch_delete)
	    log.Log( log.Info , " delete lease \n"  )    	    	
	 

	    time.Sleep(10*time.Second)
	}

}


//====================================

/*
func Test_watch(t *testing.T){
	log.Config(  log.Debug , " test module" , "" )  

	var c etcd.Client

	if ! c.Connect( []string {"http://127.0.0.1:2379" } ) {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()



	go func(){
		prefix:=true
		eventChan := c.Watch("/a" , prefix )
		if eventChan==nil{
			fmt.Println( "failed to watch etcd server" )
			return 
		}
	    for wresp := range eventChan {
	        fmt.Printf("Watch got an message " )
	        if wresp.Canceled {
	        	fmt.Printf("error , Watch was interrupted " )
	        	break
	        }
	        for n, ev := range wresp.Events {
	            fmt.Printf(" evnet %d : %s %q : %q\n", n , ev.Type, ev.Kv.Key, ev.Kv.Value)
	            //
	        }
	        //stop watch
	        break
	    }
	    fmt.Println("end watching "  )
	}()
	time.Sleep(1*time.Second)



	if ! c.Put("/a1" , "100") {
		fmt.Println("failed to put etcd")
		t.FailNow()		
	}
	time.Sleep(10*time.Second)

}
*/


func watchCallBacker(evnet etcd.EventWatch , key string, newVal string , oldVal string ){
	if evnet == etcd.EventPut {
		log.Log( log.Info , "got put event  key=%s newvalue=%s , oldval=%s \n" , key , newVal ,oldVal ) 
	}else if evnet == etcd.EventDelete {
		log.Log( log.Info , "got delete event  %s \n" , key  ) 
	}else{
		log.Log( log.Err , "got unknown event  type=%q \n" , evnet )
	}
}


func Test_watch2(t *testing.T){
	log.Config(  log.Debug , " test module" , "" )  

	var c etcd.Client

	if ! c.Connect( []string {"http://127.0.0.1:2379" } ) {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()


	ch_close_watch:=c.WatchByHandler( "/a1" , true , watchCallBacker )
	if ch_close_watch==nil{
		fmt.Println(  "failed to watch" )
		t.FailNow()		
	}

	time.Sleep(1*time.Second)

	if ! c.Put("/a1" , "100") {
		fmt.Println("failed to put etcd")
		t.FailNow()		
	}
	time.Sleep(5*time.Second)


	if ! c.Put("/a1" , "110") {
		fmt.Println("failed to put etcd")
		t.FailNow()		
	}
	time.Sleep(5*time.Second)


	if ! c.Delete("/a1" , false ) {
		fmt.Println("failed to delete ")
		t.FailNow()			
	}
	time.Sleep(5*time.Second)

	close(ch_close_watch)
	fmt.Println("close watch-----------------------")

	time.Sleep(30*time.Second)

}

//====================================



func Test_lock(t *testing.T){
	log.Config(  log.Debug , " test module" , "" )  

	var c etcd.Client

	if ! c.Connect( []string {"http://127.0.0.1:2379" } ) {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()


	//ch_close:=c.TryLock("ccc" , 3  )
	ch_close , wait_finish_closing  :=c.TryLock("ccc" , 0  )
	if ch_close==nil {
		fmt.Println(  "failed to lock" )
		t.FailNow()
	}

	fmt.Println(  "get lock" )


	if ! c.Put("/a1" , "110") {
		fmt.Println("failed to put etcd")
		t.FailNow()		
	}



	time.Sleep(10*time.Second)

	close(ch_close)
	<-wait_finish_closing

	time.Sleep(10*time.Second)

}


//====================================


func Test_elect_get(t *testing.T){
	log.Config(  log.Debug , " test module" , "" )  

	var c etcd.Client

	if ! c.Connect( []string {"http://127.0.0.1:2379" } ) {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()

	topic:="mytopic"



	leader , ok:= c.GetElectLeader( topic  )
	if !ok {
		fmt.Println(  "failed to get the leader" )
		t.FailNow()
	}

	fmt.Println(  "found leader , with name=" , leader )




}



func Test_try_elect(t *testing.T){
	log.Config(  log.Debug , " test module" , "" )  

	var c etcd.Client

	if ! c.Connect( []string {"http://127.0.0.1:2379" } ) {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()

	topic:="mytopic"

	//sh_close:=c.ElectLeader( topic  , "host_test" , 2 )
	sh_close , wait_finish_closing :=c.ElectLeader( topic  , "host_test" , 0 )

	if sh_close==nil {
		fmt.Println(  "failed to elect for " , topic )
		t.FailNow()		
	}
	fmt.Println(  "be the leader for " , topic )

	time.Sleep(20*time.Second)

	leader , ok:= c.GetElectLeader( topic  )
	if !ok {
		fmt.Println(  "failed to get the leader" )
		t.FailNow()
	}

	fmt.Println(  "found leader , with name=" , leader )


	close(sh_close)
	<-wait_finish_closing
	time.Sleep(10*time.Second)


}


//============
func Test_txn_exec (t *testing.T){

	log.Config(  log.Debug , " test module" , "" )  

	var c etcd.Client

	if ! c.Connect( []string {"http://127.0.0.1:2379" } ) {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()


	// 注意：不能对同一个 key 做多个 put 和 delete 操作 
	ok := c.TxnExec( []etcd.TxnOpStruct {
				etcd.TxnOpPut("v1" , "110") ,
				etcd.TxnOpPut("v2" , "120") ,
				etcd.TxnOpGet("v4" ) ,
				etcd.TxnOpDelete("v3" ) ,
			})
	if !ok {
		fmt.Println(  "failed to Test_txn_exec " )		
		t.FailNow()
	}


	time.Sleep(10*time.Second)


}



func Test_txn_compare (t *testing.T){

	log.Config(  log.Debug , " test module" , "" )  

	var c etcd.Client

	if ! c.Connect( []string {"http://127.0.0.1:2379" } ) {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()


	if ! c.Put("v1" , "100") {
		fmt.Println("failed to put etcd")
		t.FailNow()		
	}
	if ! c.Put("v2" , "110") {
		fmt.Println("failed to put etcd")
		t.FailNow()		
	}


	// 注意：不能对同一个 key 做多个 put 和 delete 操作 

	cmplist  := []etcd.TxnCmpStruct {
		etcd.TxnCompare(etcd.Value("v1"), "=", "100") ,
		etcd.TxnCompare(etcd.Value("v2"), "!=", "111") ,
		etcd.TxnCompare(etcd.Value("v2"), ">", "100") ,
		etcd.TxnCompare(etcd.Value("v2"), "<", "200") ,

	}
	thenlist :=[]etcd.TxnOpStruct {
				etcd.TxnOpPut("flag" , "true") ,
			}
	elselist:=[]etcd.TxnOpStruct {
				etcd.TxnOpPut("flag" , "false") ,
			}
	result, ok := c.TxnExecCmpValue(  cmplist , thenlist , elselist )
	//c.TxnExecCmpValue(  cmplist , thenlist , nil )
	if ok {
		if result {
			fmt.Println(  "execute then list " )		
		}else{
			fmt.Println(  "execute else list " )		
		}

	}else{
		fmt.Println(  "failed to Test_txn_exec " )		
		t.FailNow()

	}


	time.Sleep(10*time.Second)


}


