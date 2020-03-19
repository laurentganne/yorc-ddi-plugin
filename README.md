# Yorc DDI plugin

The Yorc DDI plugin implements a Yorc ([Ystia orchestrator](https://github.com/ystia/yorc/)) plugin as described in [Yorc documentation](https://yorc.readthedocs.io/en/latest/plugins.html), allowing the orchestrator to use LEXIS DDI (Distributed Data Infrastructure) API to manage data transfers.

## To build this plugin

You need first to have a working [Go environment](https://golang.org/doc/install).
Then to build, execute the following instructions:

```
mkdir -p $GOPATH/src/github.com/laurentganne
cd $GOPATH/src/github.com/laurentganne
git clone https://github.com/laurentganne/yorc-ddi-plugin
cd yorc-ddi-plugin
make
```

The plugin is then available at `bin/ddi-plugin`.

## Licensing

This plugin is licensed under the [Apache 2.0 License](LICENSE).
