param($installPath, $toolsPath, $package, $project)

# Get the path to the web.config file
$xmlPath = $project.Properties.Item('FullPath').Value
$xmlPath = $xmlPath + 'web.config'

write-host "Loading web.config file from project"
$doc = new-object System.Xml.XmlDocument
$doc.Load($xmlPath)

$vppLocations = ("Global", "PageFiles")

foreach ($vppLocation in $vppLocations)
{
	write-host "Reordering system.Webserver httphandlers in $vppLocation location element"
	$locations = $doc.get_DocumentElement().location | ? { $_.Path -eq $vppLocation}
	$handlers = $locations."system.WebServer".handlers
	$wildcard = ($handlers.add | ? { $_.path -eq "*"})
	$handlers.RemoveChild($wildcard) | out-null
	$handlers.InsertAfter($wildcard, $handlers.LastChild) | out-null
}

write-host "Saving updated web.config to project"
$doc.Save($xmlPath)