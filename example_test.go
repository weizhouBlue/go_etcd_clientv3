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

	if err:= c.Connect( []string {"https://10.6.185.150:12379" } ) ; err!=nil {
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
	if err:= c.Connect( nil ) ; err!=nil {
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
	var err error
	var val string

	if err:= c.Connect( []string {"http://127.0.0.1:2379" } ) ; err!=nil  {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()



	val , err = c.Get("/a20" )
	if err!=nil  {
		fmt.Println("failed to get ")
		t.FailNow()		
	}
	if len(val)==0 {
		fmt.Printf("key is empty \n" )
	}else{
		fmt.Printf("succeeded to get %s \n" , val )
	}



	if v , err := c.GetPrefix("/bbbb") ; err!=nil {
		if v==nil {
			fmt.Printf("prefix is empty  \n" )
		}else{
			fmt.Printf("succeeded to GetPrefix %v \n" , v )
		}
	}else{
		fmt.Println("failed to GetPrefix ")
		t.FailNow()			
	}


	if v , err := c.GetListKey( []string{"/a1" , "/a2" , "/a3" }  ) ; err==nil {
			fmt.Printf("succeeded to GetListKey = %+v \n" , v )
	}else{
		fmt.Println("failed to GetListKey ")
		t.FailNow()			
	}




	if  c.Put("/a1" , "100") != nil {
		fmt.Println("failed to put etcd")
		t.FailNow()		
	}
	if  c.Put("/a2" , "200") != nil {
		fmt.Println("failed to put etcd")
		t.FailNow()		
	}
	fmt.Println("succeeded to put etcd")



	val , err = c.Get("/a1" )
	if err!=nil {
		fmt.Println("failed to get ")
		t.FailNow()		
	}
	if len(val)==0 {
		fmt.Printf("key is empty \n" )
	}else{
		fmt.Printf("succeeded to get %s \n" , val )
	}



	if v , err := c.GetPrefix("/a") ; err==nil {
		if v==nil {
			fmt.Printf("prefix is empty  \n" )
		}else{
			fmt.Printf("succeeded to GetPrefix %v \n" , v )
		}
	}else{
		fmt.Println("failed to GetPrefix ")
		t.FailNow()			
	}



	if  err:=c.Delete("/a10" , false ) ; err==nil {
		fmt.Printf("succeeded to delete a10 \n" )
	}else{
		fmt.Println("failed to delete ")
		t.FailNow()			
	}

}



func Test_getPrefix(t *testing.T) {
	log.Config(  log.Debug , " test module" , "" )  

	var c etcd.Client

	if err:= c.Connect( []string {"http://127.0.0.1:2379" } ) ; err!=nil  {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()

/*
针对 etcd 上的 各个key 是按照 层级 定义的 ， 进行 解析，返回带有层级的 object
for example: etcd上 多个 key 和 其值 为如下 
    /t/a/b3  500
    /t/a/b2  400
    /t/a/b   300
    /t/mm/b5 500
那么，本函数按照key的目录级别，返回  
    map[a:map[b:300 b2:400 b3:500] mm:map[b5:500]]

    注意，如果有两个key 中，对于某个层级是 目录还是最终的文件名  出现了分歧，会自动 忽略 其作为 文件的 case
        /t/mm/b5 500
        /t/mm    600      这种key会被忽略记录    
*/
	if result , err:=c.GetPrefixReturnObj("/t" , true ) ; err!=nil {
		fmt.Println( "err : " , err )
	}else{
		fmt.Println( "ok : " , result )
	}



}




func Test_getPrefixTop(t *testing.T) {
	log.Config(  log.Debug , " test module" , "" )  

	var c etcd.Client

	if err:= c.Connect( []string {"http://127.0.0.1:2379" } ) ; err!=nil  {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()


	if dirs , keys , err:=c.GetPrefixReturnLevelName("/t" , 1 , true ) ; err!=nil {
		fmt.Println( "err : " , err )
	}else{
		fmt.Println( "dirs : " , dirs )
		fmt.Println( "keys : " , keys )

	}


	if dirs , keys , err:=c.GetPrefixReturnLevelName("/t" , 2 , true ) ; err!=nil {
		fmt.Println( "err : " , err )
	}else{
		fmt.Println( "dirs : " , dirs )
		fmt.Println( "keys : " , keys )

	}

	if dirs , keys , err:=c.GetPrefixReturnLevelName("/t" , 100 , true ) ; err!=nil {
		fmt.Println( "err : " , err )
	}else{
		fmt.Println( "dirs : " , dirs )
		fmt.Println( "keys : " , keys )

	}


	if dirs , keys , err:=c.GetPrefixReturnTopName("/t" , true ) ; err!=nil {
		fmt.Println( "err : " , err )
	}else{
		fmt.Println( "dirs : " , dirs )
		fmt.Println( "keys : " , keys )

	}



}


//====================================

func Test_lease(t *testing.T){
	log.Config(  log.Debug , " test module" , "" )  
	var c etcd.Client


	if err:= c.Connect( []string {"http://127.0.0.1:2379" } ); err!=nil {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()

	if  ch_delete , info , err := c.PutWithLease( map[string]string{"la":"200" , "lb":"201"} , 5 ) ; err !=nil{
		fmt.Println("failed to PutWithLease ")
		t.FailNow()			
	}else{
		fmt.Printf( "succeeded to PutWithLease id=%+v " , info.Lease_id )

	    go func(){
	    	for v  := range info.Ch_alive_status {
	    			log.Log( log.Info , "sent a keepalive , message =%+v  \n" , v )    
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

	if err:= c.Connect( []string {"http://127.0.0.1:2379" } ) ; err!=nil {
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
	            fmt.Printf(" evnet %d : %s %+v : %+v\n", n , ev.Type, ev.Kv.Key, ev.Kv.Value)
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
		log.Log( log.Err , "got unknown event  type=%+v \n" , evnet )
	}
}


func Test_watch2(t *testing.T){
	log.Config(  log.Debug , " test module" , "" )  

	var c etcd.Client

	if err:= c.Connect( []string {"http://127.0.0.1:2379" } ) ; err!=nil {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()


	ch_close_watch , err :=c.WatchByHandler( "/a1" , true , false, true , watchCallBacker )
	if err!=nil{
		fmt.Println(  "failed to watch" )
		t.FailNow()		
	}

	time.Sleep(1*time.Second)

	if  c.Put("/a1" , "100") != nil {
		fmt.Println("failed to put etcd")
		t.FailNow()		
	}
	time.Sleep(5*time.Second)


	if  c.Put("/a1" , "110")!= nil {
		fmt.Println("failed to put etcd")
		t.FailNow()		
	}
	time.Sleep(5*time.Second)


	if err:= c.Delete("/a1" , false ) ; err!=nil{
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

	if err:= c.Connect( []string {"http://127.0.0.1:2379" } ) ; err!=nil {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()


	//ch_close:=c.TryLock("ccc" , 3  )
	ch_close , wait_finish_closing , err :=c.TryLock("ccc" , 0  )
	if err!=nil {
		fmt.Println(  "failed to lock" )
		t.FailNow()
	}

	fmt.Println(  "get lock" )


	if c.Put("/a1" , "110")!= nil {
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

	if err:=c.Connect( []string {"http://127.0.0.1:2379" } ) ; err!=nil {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()

	topic:="mytopic"



	leader , err := c.GetElectLeader( topic  )
	if err!=nil {
		fmt.Println(  "failed to get the leader" )
		t.FailNow()
	}

	fmt.Println(  "found leader , with name=" , leader )




}



func Test_try_elect(t *testing.T){
	log.Config(  log.Debug , " test module" , "" )  

	var c etcd.Client

	if err:= c.Connect( []string {"http://127.0.0.1:2379" } ) ; err!=nil {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()

	topic:="mytopic"

	//sh_close:=c.ElectLeader( topic  , "host_test" , 2 )
	sh_close , wait_finish_closing , err :=c.ElectLeader( topic  , "host_test" , 0 )

	if err!=nil {
		fmt.Println(  "failed to elect for " , topic )
		t.FailNow()		
	}
	fmt.Println(  "be the leader for " , topic )

	time.Sleep(20*time.Second)

	leader , err := c.GetElectLeader( topic  )
	if err!=nil {
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

	if err:= c.Connect( []string {"http://127.0.0.1:2379" } ) ; err!=nil {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()

	if  c.Put("v2" , "110") != nil {
		fmt.Println("failed to put etcd")
		t.FailNow()		
	}


	// 注意：不能对同一个 key 做多个 put 和 delete 操作 
	err := c.TxnExec( []interface{} {
		[]string { etcd.TxnPutKey , "flag1" , "500"},
		[]string { etcd.TxnDelKey , "flag2" },
		[]string { etcd.TxnDelKeyWithPrefix , "v" },

	})
	if err!=nil {
		fmt.Printf(  "failed to Test_txn_exec , %v" , err )		
		t.FailNow()
	}else{
		fmt.Printf(  "succeeded to Test_txn_exec \n"  )		
	}

}



func Test_txn_compare (t *testing.T){

	log.Config(  log.Debug , " test module" , "" )  

	var c etcd.Client

	if err:= c.Connect( []string {"http://127.0.0.1:2379" } ); err!=nil  {
		fmt.Println(  "failed to connect to etcd server" )
		t.FailNow()
	}
	fmt.Println( "succeeded to connect to etcd server" )
	defer c.Close()


	if  c.Put("v1" , "100")!= nil {
		fmt.Println("failed to put etcd")
		t.FailNow()		
	}
	if  c.Put("v2" , "110") != nil {
		fmt.Println("failed to put etcd")
		t.FailNow()		
	}



	//只要有一个compare是失败的，就执行else，否则执行then
	cmplist  := []etcd.TxnCmpStruct {
		etcd.TxnCompare(etcd.TxnValue("v1"), "=", "100") ,
		etcd.TxnCompare(etcd.TxnValue("v2"), "!=", "111") ,
		etcd.TxnCompare(etcd.TxnValue("v2"), ">", "100") ,
		etcd.TxnCompare(etcd.TxnValue("v2"), "<", "200") ,
	}
	// 注意：不能多个op 对同一个 key 做操作 	
	thenlist := []interface{}{
		[]string { etcd.TxnPutKey , "flag1" , "100"},
		[]string { etcd.TxnDelKey , "flag2" },
		[]string { etcd.TxnDelKeyWithPrefix , "v" },
	}

	//can do nothing
	elselist:=[]interface{}{}
	// elselist:= []interface{}{
	// 	[]string{}{ etcd.TxnPutKey , "flag1" , "100"},
	// 	[]string{}{ etcd.TxnDelKey , "flag2" },
	// }

	thenTrue, err := c.TxnExecCmpValue(  cmplist , thenlist , elselist )
	//c.TxnExecCmpValue(  cmplist , thenlist , nil )
	if err==nil {
		if thenTrue {
			fmt.Println(  "execute then list " )		
		}else{
			fmt.Println(  "execute else list " )		
		}

	}else{
		fmt.Println(  "failed to Test_txn_exec " )		
		t.FailNow()

	}


}


