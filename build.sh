mkdir -p builds
echo "*" > builds/.gitignore

cargo build --release --target x86_64-unknown-linux-gnu
cp ./target/x86_64-unknown-linux-gnu/release/sql-py ./builds/sql-py

cargo build --release --target x86_64-pc-windows-gnu
cp ./target/x86_64-pc-windows-gnu/release/sql-py.exe ./builds/sql-py.exe
