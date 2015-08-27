[void][System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.ConnectionInfo')            
[void][System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.Management.Sdk.Sfc')            
[void][System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.SMO')                    
[void][System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.SMOExtended')   #Libreria para SQL Server 2008 SMO 10.0 
#Import-Module "C:\Program Files (x86)\AWS Tools\PowerShell\AWSPowerShell\AWSPowerShell.psd1"
Import-Module "D:\AdminScripts\Modules\AWSSDK.dll"
#[System.Reflection.Assembly]::LoadWithPartialName("AWSSDK") | out-null

#***************************************
# FUNCION PARA EJECTUAR SENTECIAS SQL
#***************************************
function global:run-sql(
		[String]$sql,
		[String]$server,
		[String]$database="master"
		)
{
	$connectionString = "Server="+$server+";Database="+$database+";Trusted_Connection=yes"
	$conn = new-object System.Data.SqlClient.SqlConnection $connectionString
	$conn.Open()
	$comm = $conn.CreateCommand()
	$comm.CommandText = $sql
	$reader = $comm.ExecuteReader()

	while($reader.Read())
	{
		$row = new-object PSObject
		for($i = 0; $i -lt $reader.FieldCount; ++$i)
		{
			add-member -inputObject $row -memberType NoteProperty -name $reader.GetName($i) -value $reader.GetValue($i)
		}
		write-output $row
	}
	$reader.Close()
	$conn.Close()
}


#***********************************************************
# FUNCION PARA CREAR BACKUPS DE TODAS LAS BASES DE DATOS
#***********************************************************
#
#	Esta funcion devuelve un arreglo o Null en caso de no existir la ruta
#
function CrearBackups_ALL(
		[ValidateNotNullOrEmpty()]
        [string] 
		$BackupDirectory = "D:\SQLBackup\testBK\",	    
		        
        [ValidateNotNullOrEmpty()]
        [string] 
        $Instance="(local)",
		        
        [ValidateSet(0,1)] 
        [int]
        $SimpleBackup=0,
		
		[ValidateSet(0,1)] 
        [int]
        $DataLOG=0)

{
	$cError=0												# contador BDs Backup erróneas
	$cOk=0													# contador BDs Backup correctas
	$BDlistaErr= New-Object System.Collections.Hashtable	# lista de BDs Backup erróneas
	$BDlistaOke= New-Object System.Collections.Hashtable	# lista de BDs Backup correctas	
	$BDListaResult = New-Object System.Collections.Hashtable	# Lista del estado de los backups generados automaticamente
	$startDTM = (Get-Date)									# Inicio del tiempo de ejecución
	 

	# Verificamos el directorio de los backups
	New-Item -ErrorAction Ignore -ItemType directory -Path $BackupDirectory
	if(!(Test-Path -Path $BackupDirectory)){
		 $msgFinal= "La ruta '$BackupDirectory' no existe y no puede ser creada. Se detuvo el script y no se realizo ningun backup en este servidor. Por favor contactar inmediatamente con el DBA"
		EnviarCorreo  "[Belcorp-backups] ERROR PATH $env:computername" $msgFinal
		return $null
	}      

	# SQL Server
	$srv = New-Object Microsoft.SqlServer.Management.Smo.Server -ArgumentList $Instance  				
	$srv.ConnectionContext.StatementTimeout = 0								#Deshabilitamos monitoreo de tiempo
		
	$TipoBK="INC"
	if ($SimpleBackup -eq 0){
		$TipoBK="FULL"
	}
		
	# Copiamos bases de datos        		
	foreach ($db in $srv.Databases)            
	{
		try
		{
			$dbstatus=$db.status
			if ($dbstatus -eq "Normal"){
				If( ($db.Name -ne "tempdb") -and ($db.Name -ne "master") -and ($db.Name -ne "model") )					
				{            					
					$backup = New-Object ("Microsoft.SqlServer.Management.Smo.Backup")  
					
					$timestamp = Get-Date -format yyyyMMddHHmmss     
					$Archivo = "$($TimeStamp)_$($env:COMPUTERNAME)_$($db.Name)_$($TipoBK).bak"
					$FullArchivo = Join-Path $BackupDirectory $Archivo					
										      						 
					$backup.Action = "Database"           
					$backup.Database = $db.Name				
					write-host "Copiando " $db.Name            
					$backup.Devices.AddDevice($FullArchivo, "File")					
					$backup.BackupSetDescription = "Full backup de " + $db.Name + " " + $timestamp            
					$backup.Incremental = $SimpleBackup
					$backup.Checksum = $true
					$backup.CompressionOption=1					     
					$backup.SqlBackup($srv)			# Full backup
					
					# Creamos Backup Log si es necesario
					if ($DataLOG  -eq 1)
					{					
						# Creamos Backup Log si la base de datos no esta en modo recovery simple        
						If ($db.RecoveryModel -ne 3)
						{            
							$timestamp = Get-Date -format yyyyMMddHHmmss            
							$backup = New-Object ("Microsoft.SqlServer.Management.Smo.Backup")    

							$Archivo = "$($TimeStamp)_$($env:COMPUTERNAME)_$($db.Name)_LOG.trn"
							$FullArchivo = Join-Path $BackupDirectory $Archivo	
							
							$backup.Action = "Log"            
							$backup.Database = $db.Name    				
							$backup.Devices.AddDevice($FullArchivo, "File")

							$backup.BackupSetDescription = "Log backup de " + $db.Name + " " + $timestamp    
							$backup.Checksum = $true														         
							$backup.LogTruncation = "Truncate"	# Truncamos logs  
							$backup.CompressionOption=1			# Compresion				     
							$backup.SqlBackup($srv)				# Log backup  
						}    
					}										
					$BDListaResult.Add($db.Name,"OK")
					$cOk++						
				}
			}
			else{			
				$cError++
				$estadoErr="<p style='color:red'>$dbstatus</p>"			
				$BDListaResult.Add($db.Name,$estadoErr)
			}
		}
		catch
		{			
			$cError++
			$ExceptionMsg = "<p style='color:red'> $($_.Exception.GetType().FullName)" + ". Mensaje Exepcion: $($_.Exception.Message) </p>"
			$BDListaResult.Add($db.Name,$ExceptionMsg)
	
		}
	}

	$endDTM = (Get-Date)			# Final de la ejecución
	$tiempo_total ="$(($endDTM-$startDTM).hours) HH $(($endDTM-$startDTM).Minutes) MM $(($endDTM-$startDTM).Seconds) SS"		
	
	# Agregamos el tiempo total de ejecucion al final de la tabla de resultado	
	$BDListaResult.Add("Tiempo"," $tiempo_total ")
	# Agregamos un contador con la cantidad exito y error de backups
	$BDListaResult.Add("Contador","OK($cOK)   ERROR($cError)")
	
	return $BDListaResult
	
}




#***********************************************************
# FUNCION PARA CREAR BACKUPS DE LOS LOGS
#***********************************************************
function CrearBackups_Log(
		[ValidateNotNullOrEmpty()]
        [string] 
		$BackupDirectory = "D:\SQLBackup\BKLogsTest\",	    
		        
        [ValidateNotNullOrEmpty()]
        [string] 
        $Instance="(local)")
		
{
	$cError=0												
	$cOk=0													
	$BDlistaErr= New-Object System.Collections.Hashtable	
	$BDlistaOke= New-Object System.Collections.Hashtable	
	$startDTM = (Get-Date)									

	# Verificamos el directorio de los backups
	New-Item -ErrorAction Ignore -ItemType directory -Path $BackupDirectory
	if(!(Test-Path -Path $BackupDirectory)){
		$msgFinal= "La ruta '$BackupDirectory' no existe y no puede ser creada. Se detuvo el script y no se realizó ningun backup en este servidor"
		EnviarCorreo  "[Belcorp-backups] $env:computername" $msgFinal
		return "False"
	}      
	
	# SQL Server
	$srv = New-Object Microsoft.SqlServer.Management.Smo.Server -ArgumentList $Instance  				
	$srv.ConnectionContext.StatementTimeout = 0								
					
	# Copiamos bases de datos        		
	foreach ($db in $srv.Databases)            
	{
		try
		{			
			# VERIFICAMOS EL ESTADO DE LA BASE DE DATOS: Normal, Restoring y Offline			
			if ($db.status -eq "Normal")
			{
				# BACKUP DEL LOG SI EL MODO DE RECUPERACION NO ES SIMPLE
				If( ($db.Name -ne "tempdb") -and ($db.Name -ne "master") -and ($db.Name -ne "model") -and ($db.RecoveryModel -ne 3))
				{         					
					$timestamp = Get-Date -format yyyyMMddHHmmss       
					$backup = New-Object ("Microsoft.SqlServer.Management.Smo.Backup")            
					$PathFile = ($BackupDirectory + $timestamp +"_"+$env:COMPUTERNAME+"_"+ $db.Name + "_log_"+ ".trn")
					$backup.Action = "Log"            
					$backup.Database = $db.Name
					Write-Output "Copiando Log ... " $db.Name 				
					$backup.Devices.AddDevice($PathFile, "File")
					$backup.BackupSetDescription = "Log backup de " + $db.Name + " " + $timestamp    
					$backup.Checksum = $true														         
					$backup.LogTruncation = [Microsoft.SqlServer.Management.Smo.BackupTruncateLogType]::Truncate  # Truncamos logs  
					$backup.CompressionOption=1						
					$backup.SqlBackup($srv)			   # Log backup 
					
					$BDlistaOke.Add($db.Name,$PathFile)					
					$cOk++
				}   
			}
			else{
				$estadoErr=$db.status
				if ($db.status -eq "Restoring"){
					$estadoErr="Mirror"
				}
				if ($db.status -eq "Offline"){
					$estadoErr="Fuera de Servicio"
				}
				
				$cError++								
				$BDlistaErr.Add($db.Name,$estadoErr)
			}
			
		}
		catch
		{			
			$cError++				
			$ExceptionMsg = " Estado:'"+$db.status + "', Tipo de Excepcion: $($_.Exception.GetType().FullName)" + ". Mensaje: $($_.Exception.Message)"	
			$BDlistaErr.Add($db.Name,$ExceptionMsg)			
		}
	}
	$endDTM = (Get-Date)			# Final de la ejecución
	$tiempo_total ="$(($endDTM-$startDTM).hours) HH $(($endDTM-$startDTM).Minutes) MM $(($endDTM-$startDTM).Seconds) SS"		
	$MensajeFinal="LOS BACKUPS FUERON REALIZADOS EN <h2> $tiempo_total </h2><br><br>"
	
	if ($cOK -gt 0){
		$tablaOK = mensaje_HMTL($BDlistaOke)
		$MensajeFinal+= " <h3>BACKUPS LOGS OK</h3> <br>"+$tablaOK		
	}	
	if ($cError -gt 0){
		$tablaEr = mensaje_HMTL($BDlistaErr)
		$MensajeFinal+= " <h3 style='color:red'>BACKUPS LOGS ERROR</h3> <br>"+$tablaEr		
	}
	EnviarCorreo  ("[BACKUP LOGS] $env:computername") $MensajeFinal
	
	return "True"
}



#*******************************************************************
# FUNCION PARA VERIFICAR LA INTEGRIDAD DE TODAS LAS BASES DE DATOS
#*******************************************************************
function verifyBackup(
		[ValidateNotNullOrEmpty()]
		[String]$backupPath = "D:\SQLBackup\testBK\",
		[String]$sqlserver="localhost"
		)
 
{
	$BDlistaOk	= New-Object System.Collections.Hashtable
	$BDlistaBad	= New-Object System.Collections.Hashtable	
	$BDListaResult	= New-Object System.Collections.Hashtable
	
	$cOK2=0
	$cError2=0
	$DirError="$backupPath\Error\"
	$startDTM = (Get-Date)	
	
	# obtenemos las rutas de los backups  eje. C:\Backyp\belcorppais.bak etc..
	$files=gci -Path $backupPath | where{$_.Extension -match "bak|trn"} | select name | sort-object name	
	
	New-Item -ErrorAction Ignore -ItemType directory -Path $DirError
	
	# Bucle para verificar cada archivo .bak 																					
	foreach($file in $files)	
	{		
		write-host ("$file -> Verificando.. ")
		$filepath=$backupPath+"\" + $file.Name
		$sqlcmd="RESTORE VERIFYONLY from disk='" + $filepath+"' WITH CHECKSUM"
		  
		$cmd="SQLCMD -E -S " + $sqlserver + " -Q `" " + $sqlcmd + " `""
		$result = invoke-expression $cmd		
		$msgOk="The backup set on file 1 is valid."
		
		if ($result -eq $msgOk){					
			$BDListaResult.Add($file.Name, "<p style='color:green'> $result </p>") # OK						
			$cOK2++
		}
		else{
			$BDListaResult.Add($file.Name, "<p style='color:red'> $result </p>")  # NO PASA VERIFICACION							
			$Archivo=$file.Name
			Move-Item "$backupPath\$Archivo" $DirError		# Movemos el item erroneo
			$cError2++
			
		}
	}
	$endDTM = (Get-Date)			# Final de la ejecución
	$tiempo_total ="$(($endDTM-$startDTM).hours) HH $(($endDTM-$startDTM).Minutes) MM $(($endDTM-$startDTM).Seconds) SS"			
	
	$BDListaResult.Add("Tiempo", "$tiempo_total")  # NO PASA VERIFICACION				
	$BDListaResult.Add("Contador", "OK($cOK2)   ERROR($cError2)")  # NO PASA VERIFICACION			
	
	return $BDListaResult	
}



#******************************************
# FUNCION ENVIAR CORREO DE NOTIFICACION
#******************************************
Function EnviarCorreo()
{
	param
	(
		[String] $asunto, 
		[String] $mensaje 
	)
	try {
		#Cuenta para enviar correos
		$de = "AKIAJU5T7GIUQOAL3EFQ"
		$pass = "AldxDwG5/59W1CeNfr5PIoVMhWL+nlpJOxL8hVL8c9uD"

		#Receptores de Alertas
		$para = "alfredo.zavala@solucionesorion.com,herbert.montanez@solucionesorion.com,martin.hidalgo@solucionesorion.com,alex.tejada@solucionesorion.com"
		#$para = "alfredo.zavala@solucionesorion.com"

		$mail = New-Object System.Net.Mail.MailMessage
		$mail.From = New-Object System.Net.Mail.MailAddress("aws-belcorp@solucionesorion.com")
		$mail.To.Add($para)
		$mail.Subject = $asunto
		$mail.IsBodyHtml = $true	
		$mail.Body = $mensaje

		#servidor de correo
		$smtp = New-Object System.Net.Mail.SmtpClient -ArgumentList "email-smtp.us-east-1.amazonaws.com"
		$smtp.EnableSsl = $true
		$smtp.Port = "587"
		$smtp.Credentials = New-Object System.Net.NetworkCredential -ArgumentList $de,$pass		
		$smtp.Send($mail)	
	}
	catch {
		Write-Host $_.Exception.Message #registrar evento de comunicaciones en logevent
	}
}



#******************************************
# FUNCION PARA CREAR TABLA HTML
#******************************************
function mensaje_HMTL(		
		[System.Collections.Hashtable] $arreglo 
		)
{

	$tabla="<table cellspacing='2'> 	
			<tr style='background:#556C86;color:white'>
				<th>Nro</th><th>Item</th><th>Description</th></tr>"
			
	$i=0		
	foreach($item in $arreglo.keys)
	{
		$i++
		$descripcion=$arreglo.Item($item).ToLower()
		$item=$item.ToLower()
		
		if (($item -ne "Contador") -and ($item -ne "Tiempo")){		
			$tabla+= "<tr style='background:#d5edeb'>
				<th> $i </th><th> $item </th><td> $descripcion </td></tr>"			
		}
		else{
			if ($item -eq "Contador"){
				$contador=$descripcion}
			if ($item -eq "Tiempo"){
				$Tiempo=$descripcion}
		}
	}
	
	$tabla+= "<tr style='background:orange'><th> - </th><th> Contador </th><td> $contador </td></tr>"
	$tabla+= "<tr style='background:orange'><th> - </th><th> Tiempo </th><td> $Tiempo </td></tr>"	
	$tabla+= "</table>"	
	return $tabla
}



#******************************************
# FUNCION ELIMINAR LOS ARCHIVOS
#******************************************
function eliminar_archivos(
		[String] $TargetFolder 
		)
{		
	$Now = Get-Date
	$Days = "1" #Días de permanencia	
	$Extension = @("*.bak","*.trn")
	$LastWrite = $Now.AddDays(-$Days)

	$Files = Get-Childitem $TargetFolder -Include $Extension -Recurse | Where {$_.LastWriteTime -le "$LastWrite"}

	foreach ($File in $Files) 
	{
		if ($File -ne $NULL)
		{
			write-host "Deleting File $File" -ForegroundColor "DarkRed"
			Remove-Item $File.FullName | out-null
		}		
	}
	
}


#******************************************
# FUNCION SUBIR ARCHIVOS A AMAZON
#******************************************
function Write-AWSS3
{
	param
    (
		[String]$AWSAccessKey,
		[String]$AWSSecretKey,
		[String]$AWSRegion, 
		[String]$FilePath,
		[String]$BucketName,
		[String]$Key
    )
	try
	{
	    [System.Reflection.Assembly]::LoadWithPartialName("AWSSDK") | out-null
		$endpoint = [Amazon.RegionEndpoint]::GetBySystemName($AWSRegion)
		
		$file = Get-ChildItem $FilePath
		$AWSS3Client = new-object Amazon.S3.Transfer.TransferUtility($AWSAccessKey, $AWSSecretKey, $endpoint)
		$AWSRequest = new-object Amazon.S3.Transfer.TransferUtilityUploadRequest
		$AWSRequest.WithBucketName($BucketName)
		$AWSRequest.WithKey($Key + "/" + $file.Name)
		$AWSRequest.WithFilePath($FilePath)
		$AWSRequest.WithCannedACL("Private")
		$AWSResponse = $AWSS3Client.Upload($AWSRequest)
		$AWSS3Client.Dispose()

	}
		catch 
		{
			$ErrorMessage = $_.Exception.Message
	    	$FailedItem = $_.Exception.ItemName
			[System.Diagnostics.EventLog]::WriteEntry("Script AWS", $_.Exception.Message, "Error");			
            #Write-Host $ErrorMessage	    	
		}	
}

function Enviar_S3(
		[string] $Ruta="D:\SQLBackup\testBK\",
		[string] $S3Bucket,
		[string] $S3BackupDirInBucket,
		[string] $S3Key,
		[string] $S3SecretKey,
		[string] $S3Region
		)
		
{
	$cOk=0
	$cError=0
		
	$BDListaResult = New-Object System.Collections.Hashtable		
	
	#Initialize-AWSDefaults -AccessKey $S3Key -SecretKey $S3SecretKey -Region $S3Region		
	$files=gci -Path $Ruta | where{$_.extension -eq ".trn" -or $_.extension -eq ".bak"} | select name | sort-object name
 
	$startDTM = (Get-Date)
	
	Foreach ($file in $files)
	{	
		$BackupFile=$Ruta+"\" + $file.Name	
		try
		{
			Write-AWSS3 "$S3Key" "$S3SecretKey" "$S3Region" "$BackupFile" "$S3Bucket" "$S3BackupDirInBucket"			
			$BDListaResult.Add($file.Name,"Amazon OK")					
			$cOk++
		}
		catch 
		{																
			$BDListaResult.Add($file.Name,"<p style='color:red'> Error -> $_.Exception.Message </p>")			
			$cError++
		}
	}	
	
	$endDTM = (Get-Date)			# Final de la ejecución
	$tiempo_total ="$(($endDTM-$startDTM).hours) HH $(($endDTM-$startDTM).Minutes) MM $(($endDTM-$startDTM).Seconds) SS"	
				
	$BDListaResult.Add("Tiempo", "$tiempo_total")  
	$BDListaResult.Add("Contador", "OK($cOK)    ERROR($cError)")  

	$Resultado = mensaje_HMTL($BDListaResult)
	return  "@ $Resultado"
}


#*****************************************************************************
# FUNCION PARA MONITOREAR LOS SERVICIOS DE SQL SERVER , MSSQLSERVER, AGENT
#*****************************************************************************
function ServiciosSQL(
		[string] $motor="MSSQLSERVER",
		[string] $agente="SQLSERVERAGENT"
		)
		
{	
	$result=""
	$Lservicios = Get-service -displayname *SQL* | Where-Object {$_.status -eq "stopped"}
	foreach($ser in $Lservicios)
	{
		if (($ser.Name -eq $motor)){
			$result += $motor+"@"
		}
		if (($ser.Name -eq $agente)){
			$result += $agente+"@"
		}		
	}
	return $result
}


Export-ModuleMember -function Enviar_S3
Export-ModuleMember -function Write-AWSS3
Export-ModuleMember -function EnviarCorreo
Export-ModuleMember -function CrearBackups_ALL
Export-ModuleMember -function CrearBackups_Log
Export-ModuleMember -function verifyBackup
Export-ModuleMember -function mensaje_HMTL
Export-ModuleMember -function eliminar_archivos
Export-ModuleMember -function UPLOAD_S3
Export-ModuleMember -function ServiciosSQL


