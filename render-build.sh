#!/bin/bash

curl -sSL https://dot.net/v1/dotnet-install.sh > dotnet-install.sh
chmod +x dotnet-install.sh

./dotnet-install.sh --channel 10.0

export PATH="$PATH:$HOME/.dotnet"

dotnet publish Arkhos.Web/Arkhos.Web.csproj -c Release -o dist