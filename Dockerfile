FROM rust:1.90
LABEL authors="vaughnw128"

COPY . .

RUN cargo build --release

CMD ["./target/release/mc-status-rs"]