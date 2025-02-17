mkdir -p builds
echo "*" > builds/.gitignore

cargo build --release --target x86_64-unknown-linux-gnu
cp ./target/x86_64-unknown-linux-gnu/release/sql-infer ./builds/sql-infer

cargo build --release --target x86_64-pc-windows-gnu
cp ./target/x86_64-pc-windows-gnu/release/sql-infer.exe ./builds/sql-infer.exe
