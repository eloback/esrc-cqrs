{ pkgs, lib, ... }:
{
  languages.rust.enable = true;
  packages = with pkgs; [
    openssl
    natscli
  ];
  services.nats = {
    enable = true;
    jetstream.enable = true;
  };
}
