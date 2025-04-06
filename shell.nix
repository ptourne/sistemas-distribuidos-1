{ pkgs ? import <nixpkgs> {} }:

let
  shell = pkgs.mkShell {
    packages = with pkgs; [
        glibcLocales

        # Go
        go # v1.23.7
    ];

    shellHook = ''
        # Prefix the prompt with the name of the shell.
        export PS1="[$name] $PS1"

        # Setup the Go environment.
        export GOPATH=$HOME/go
        export PATH=$GOPATH/bin:$PATH

        echo "╭───────────────────────────────────────────────╮"
        echo "│ Welcome to your devshell!                     │"
        echo "│ ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺                      │"
        echo "│ Run 'exit' to go back you your system's shell │"
        echo "╰───────────────────────────────────────────────╯"
        '';
    };

in
shell
