import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

const config = new pulumi.Config();
const domain = config.require("domain");

// GitHub Pages IP addresses
const githubPagesIps = [
  "185.199.108.153",
  "185.199.109.153",
  "185.199.110.153",
  "185.199.111.153",
];

// Hosted zone for pgmt
const hostedZone = new aws.route53.Zone("pgmt-zone", {
  name: domain,
});

// A records for apex domain pointing to GitHub Pages
const apexRecords = new aws.route53.Record("apex", {
  zoneId: hostedZone.zoneId,
  name: domain,
  type: "A",
  ttl: 300,
  records: githubPagesIps,
});

// CNAME for www subdomain
const wwwRecord = new aws.route53.Record("www", {
  zoneId: hostedZone.zoneId,
  name: `www.${domain}`,
  type: "CNAME",
  ttl: 300,
  records: [domain],
});

// Exports
export const zoneId = hostedZone.zoneId;
export const nameServers = hostedZone.nameServers;
export const domainName = domain;
