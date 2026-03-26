{ inputs, ... }:

{
  perSystem = { pkgs, lib, ... }: {
    devshells.default = {
      packages = with pkgs; [
        # --- Core build deps ---
        openssl
        pkg-config

        gcc14
        lld
        protobuf
        cyrus_sasl

        gnumake
        cmake
        maven
        jdk17_headless

        tmux
        postgresql
        patchelf
      ];

      env = [
        {
          name = "CMAKE_PREFIX_PATH";
          value = "${pkgs.openssl.dev}";
        }
        {
          name = "OPENSSL_NO_VENDOR";
          value = "1";
        }
        {
          name = "OPENSSL_ROOT_DIR";
          value = "${pkgs.openssl.out}";
        }
        {
          name = "OPENSSL_LIB_DIR";
          value = "${pkgs.openssl.out}/lib";
        }
        {
          name = "OPENSSL_INCLUDE_DIR";
          value = "${pkgs.openssl.dev}/include";
        }
        {
          name = "LD_LIBRARY_PATH";
          value = lib.makeLibraryPath [
            pkgs.openssl
            pkgs.cyrus_sasl
          ];
        }
      ];
    };
  };
}