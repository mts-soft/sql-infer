mkdir -p builds
echo "*" > builds/.gitignore

cargo build --release --bin sql-infer-cli --target x86_64-unknown-linux-gnu
cp ./target/x86_64-unknown-linux-gnu/release/sql-infer-cli ./builds/sql-infer-linux

cargo build --release --bin sql-infer-cli --target x86_64-pc-windows-gnu
cp ./target/x86_64-pc-windows-gnu/release/sql-infer-cli.exe ./builds/sql-infer-win.exe