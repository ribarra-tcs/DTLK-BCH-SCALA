package cl.bancochile.fdd.tables.dim  
 
import com.huemulsolutions.bigdata.common._ 
import com.huemulsolutions.bigdata.control._ 
import com.huemulsolutions.bigdata.tables._ 
import com.huemulsolutions.bigdata.dataquality._ 
import org.apache.spark.sql.types._ 
 
 
class tbl_fac_operacion(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_Table(huemulBigDataGov, Control) with Serializable { 
  /**********   C O N F I G U R A C I O N   D E   L A   T A B L A   ****************************************/ 
   

 // this.setTableType(huemulType_Tables.Transaction) 
  this.setTableType(huemulType_Tables.Transaction) 

 
  //Base de Datos en HIVE donde sera creada la tabla 
  this.setDataBase(huemulBigDataGov.GlobalSettings.DIM_DataBase) 
 
  //Tipo de archivo que sera almacenado en HDFS 
  this.setStorageType(huemulType_StorageType.PARQUET) 

	
  //Ruta en HDFS donde se guardara el archivo PARQUET 
  this.setGlobalPaths(huemulBigDataGov.GlobalSettings.DIM_BigFiles_Path) 
 
 
  //Frecuencia de actualizacion 
  this.setFrequency(huemulType_Frequency.MONTHLY) 
   
  /**********   S E T E O   I N F O R M A T I V O   ****************************************/ 
  //Descripcion de la fuente 
  this.setDescription("") 
 
  //Nombre del contacto de negocio  
  this.setBusiness_ResponsibleName("") 
 
  //Nombre del contacto de TI 
  this.setIT_ResponsibleName("") 
    
  
  /**********   S E G U R I D A D   ****************************************/ 
  //Solo estos package y clases pueden ejecutar en modo full, si no se especifica todos pueden invocar 
//  this.WhoCanRun_executeFull_addAccess("process_fac_operacion", 
//                                       "cl.bancochile.fdd.dim.sgt.process") 
  //Solo estos package y clases pueden ejecutar en modo solo Insert, si no se especifica todos pueden invocar 
//  this.WhoCanRun_executeOnlyInsert_addAccess("process_fac_operacion", 
//                                        "cl.bancochile.fdd.dim.sgt.process") 
  //Solo estos package y clases pueden ejecutar en modo solo Update, si no se especifica todos pueden invocar 
//  this.WhoCanRun_executeOnlyUpdate_addAccess("process_fac_operacion", 
//                                        "cl.bancochile.fdd.dim.sgt.process") 
   
/**********   Columns Information   ****************************************/ 

   
//Aplicar DataQuality de Integridad Referencial
   val tbl_dim_periodo = new tbl_dim_periodo(huemulBigDataGov, Control )  

   val fk1_tbl_fac_operacion = new huemul_Table_Relationship (tbl_dim_periodo,false)

    val periodo_mes = new huemul_Columns (StringType,true,"") 
    periodo_mes.setNullable(false) 
    periodo_mes.setIsPK(true) 
    periodo_mes.setPartitionColumn(1, dropBeforeInsert = true, oneValuePerProcess = false)


  fk1_tbl_fac_operacion.AddRelationship (tbl_dim_periodo.period_num_periodo,periodo_mes)

 
   val tbl_dim_institution = new tbl_dim_institution(huemulBigDataGov,Control)

   val fk2_tbl_fac_operacion = new huemul_Table_Relationship(tbl_dim_institution,false)


   val id_interfaz = new huemul_Columns (IntegerType,true,"") 
    id_interfaz.setNullable(false) 
    id_interfaz.setIsPK(true) 
  

    fk2_tbl_fac_operacion.AddRelationship (tbl_dim_institution.Id_institucion,id_interfaz)

    val id_institucion = new huemul_Columns (IntegerType,true,"") 
    id_institucion.setNullable(false) 
    id_institucion.setIsPK(true)    
 


   val tbl_dim_cuentas_contables = new tbl_dim_cuentas_contables(huemulBigDataGov,Control)


  val fk3_tbl_fac_operacion = new huemul_Table_Relationship(tbl_dim_cuentas_contables,false)

    
    val id_cc = new huemul_Columns (StringType,true,"")
    id_cc.setNullable(false)
    id_cc.setIsPK(true)


  fk3_tbl_fac_operacion.AddRelationship (tbl_dim_cuentas_contables.id_Cuenta_Contable,id_cc)

    val cc_mon = new huemul_Columns (DoubleType,true,"") 
cc_mon.setNullable(true) 
    cc_mon.setIsPK(false) 
   
    

 
     
  this.ApplyTableDefinition() 
} 


