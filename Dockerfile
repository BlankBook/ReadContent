FROM busybox
ADD main /
EXPOSE 8080
CMD ["./main"]
