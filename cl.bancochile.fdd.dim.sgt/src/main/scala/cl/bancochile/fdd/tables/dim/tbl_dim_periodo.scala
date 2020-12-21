package cl.bancochile.fdd.tables.dim 
 
import com.huemulsolutions.bigdata.common._ 
import com.huemulsolutions.bigdata.control._ 
import com.huemulsolutions.bigdata.tables._ 
import com.huemulsolutions.bigdata.dataquality._ 
import org.apache.spark.sql.types._ 
 
 
class tbl_dim_periodo (huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_Table(huemulBigDataGov, Control) with Serializable { 
  /**********   C O N F I G U R A C I O N   D E   L A   T A B L A   ****************************************/ 
  //Tipo de tabla, Master y Reference son catalogos sin particiones de periodo 
  this.setTableType(huemulType_Tables.Master) 
 
  //Base de Datos en HIVE donde sera creada la tabla 
  this.setDataBase(huemulBigDataGov.GlobalSettings.DIM_DataBase) 
 
  //Tipo de archivo que sera almacenado en HDFS 
  this.setStorageType(huemulType_StorageType.PARQUET) 
 
  //Ruta en HDFS donde se guardara el archivo PARQUET 
  this.setGlobalPaths(huemulBigDataGov.GlobalSettings.DIM_BigFiles_Path) 
 
  //Ruta en HDFS especifica para esta tabla (Globalpaths / localPath) 
 // this.setLocalPath("sgt/") 
 
  //Frecuencia de actualizacion 
  this.setFrequency(huemulType_Frequency.NOT_SPECIFIED) 
   
  /**********   S E T E O   I N F O R M A T I V O   ****************************************/ 
  //Descripcion de la fuente 
  this.setDescription("") 
 
  //Nombre del contacto de negocio  
  this.setBusiness_ResponsibleName("") 
 
  //Nombre del contacto de TI 
  this.setIT_ResponsibleName("") 
    
  
  /**********   S E G U R I D A D   ****************************************/ 
  //Solo estos package y clases pueden ejecutar en modo full, si no se especifica todos pueden invocar 
//  this.WhoCanRun_executeFull_addAccess("process_dim_periodo", 
//                                       "cl.bancochile.fdd.dim.sgt.process") 
  //Solo estos package y clases pueden ejecutar en modo solo Insert, si no se especifica todos pueden invocar 
//  this.WhoCanRun_executeOnlyInsert_addAccess("process_dim_periodo", 
//                                        "cl.bancochile.fdd.dim.sgt.process") 
  //Solo estos package y clases pueden ejecutar en modo solo Update, si no se especifica todos pueden invocar 
//  this.WhoCanRun_executeOnlyUpdate_addAccess("process_dim_periodo", 
//                                        "cl.bancochile.fdd.dim.sgt.process") 
   
/**********   Columns Information   ****************************************/ 
  //If table is Transaction. period must be create 
    
 
   
    val period_dia = new huemul_Columns (StringType,true,"") 
    period_dia.setNullable(false) 
    period_dia.setIsPK(true) 
   
    val period_fec_periodo = new huemul_Columns (StringType,true,"") 
    period_fec_periodo.setNullable(true) 
    period_fec_periodo.setIsPK(false) 
	
	 val period_num_periodo = new huemul_Columns (IntegerType,true,"") 
    period_num_periodo.setNullable(true) 
    period_num_periodo.setIsPK(false) 
   
   
    val period_nom_dia = new huemul_Columns (StringType,true,"") 
    period_nom_dia.setNullable(true) 
    period_nom_dia.setIsPK(false) 
   
   
    val period_nom_dia_abr = new huemul_Columns (StringType,true,"") 
    period_nom_dia_abr.setNullable(true) 
    period_nom_dia_abr.setIsPK(false) 
   
   
    val period_num_dia_sem = new huemul_Columns (IntegerType,true,"") 
    period_num_dia_sem.setNullable(true) 
    period_num_dia_sem.setIsPK(false) 
   
   
    val period_num_dia_mes = new huemul_Columns (IntegerType,true,"") 
    period_num_dia_mes.setNullable(true) 
    period_num_dia_mes.setIsPK(false) 
   
   
    val period_num_dia_anio = new huemul_Columns (IntegerType,true,"") 
    period_num_dia_anio.setNullable(true) 
    period_num_dia_anio.setIsPK(false) 
   
	
	val period_num_sem_mes = new huemul_Columns (IntegerType,true,"") 
    period_num_sem_mes.setNullable(true) 
    period_num_sem_mes.setIsPK(false) 
   
   val period_num_sem_anio = new huemul_Columns (IntegerType,true,"") 
    period_num_sem_anio.setNullable(true) 
    period_num_sem_anio.setIsPK(false) 
   
    val period_nom_mes = new huemul_Columns (StringType,true,"") 
    period_nom_mes.setNullable(true) 
    period_nom_mes.setIsPK(false) 
	
	val period_nom_mes_abr = new huemul_Columns (StringType,true,"") 
    period_nom_mes_abr.setNullable(true) 
    period_nom_mes_abr.setIsPK(false) 
	
	val period_num_mes = new huemul_Columns (IntegerType,true,"") 
    period_num_mes.setNullable(true) 
    period_num_mes.setIsPK(false) 
	
	val period_num_anio = new huemul_Columns (IntegerType,true,"") 
    period_num_anio.setNullable(true) 
    period_num_anio.setIsPK(false) 
   
   
   val period_ind_habil = new huemul_Columns (IntegerType,true,"") 
    period_ind_habil.setNullable(true) 
    period_ind_habil.setIsPK(false) 
   
   
   
   
   
   
   
  
    
  //-**********Ejemplo para aplicar DataQuality de Integridad Referencial 
 
     
  this.ApplyTableDefinition() 
} 
