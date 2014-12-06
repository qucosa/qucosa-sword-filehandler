# Qucosa METS Filehandler for SWORD Server

Implements a file handler class for [sword-fedora](https://github.com/slub/sword-fedora)
SWORD server which handles deposits of media type `application/vnd.qucosa.mets+xml`.

## Building

The Qucosa SWORD file handler is an extension to sword-fedora. For compilation the sword-fedora
classes have to be installed in your local Maven repository.

To achieve this, simply clone [sword-fedora](https://github.com/slub/sword-fedora) and run
`mvn install`.

## Deployment

Make sure the Qucosa SWORD file handler Jar is present in your sword-fedora deployments class path.
Then add `<handler class="org.purl.sword.server.fedora.fileHandlers.QucosaMETSFileHandler"/>` to the
`<file_handlers` section in the `properties.xml` you use for configuring the SWORD server.

