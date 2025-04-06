{
  description = "Nix shells for development";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/24.11";
  };

  outputs = { self, nixpkgs, ... } @ inputs:
  let
    pkgs = nixpkgs.legacyPackages.x86_64-linux;
  in
  {
    packages.x86_64-linux.default =
        import ./shell.nix { inherit pkgs; };
  };
}
